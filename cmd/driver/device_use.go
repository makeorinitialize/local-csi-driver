// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	csiLVM "local-csi-driver/internal/csi/core/lvm"
	"local-csi-driver/internal/deviceuse"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

type deviceUseConfig struct {
	Scope            deviceuse.Scope   `json:"scope"`
	JournalDirectory string            `json:"journalDirectory"`
	ReaderKubeconfig string            `json:"readerKubeconfig"`
	Endpoint         string            `json:"endpoint"`
	ServerCertFile   string            `json:"serverCertFile"`
	ServerKeyFile    string            `json:"serverKeyFile"`
	ClientCAFile     string            `json:"clientCAFile"`
	Grants           []deviceuse.Grant `json:"grants"`
}

func protectedFile(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 || st.Sys().(*syscall.Stat_t).Uid != 0 {
		return errors.New("device-use configuration must be root-owned and not group/world writable")
	}
	return nil
}

func configureDeviceUse(path, node string, manager lvmMgr.Manager) (*deviceuse.Domain, *useListener, error) {
	if path == "" {
		return nil, nil, nil
	}
	if err := protectedFile(path); err != nil {
		return nil, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 128*1024))
	d.DisallowUnknownFields()
	var cfg deviceUseConfig
	if err := d.Decode(&cfg); err != nil {
		return nil, nil, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, nil, errors.New("trailing device-use configuration")
	}
	if !cfg.Scope.Valid() || cfg.Scope.Driver != csiLVM.DriverName || cfg.Scope.Node != node ||
		cfg.Endpoint == "" || !filepath.IsAbs(cfg.JournalDirectory) || filepath.Clean(cfg.JournalDirectory) != cfg.JournalDirectory {
		return nil, nil, errors.New("exact device-use node/driver scope and persistent journal required")
	}
	for _, file := range []string{cfg.ReaderKubeconfig, cfg.ServerCertFile, cfg.ServerKeyFile, cfg.ClientCAFile} {
		if err := protectedFile(file); err != nil {
			return nil, nil, err
		}
	}
	client, err := useReader(cfg.ReaderKubeconfig)
	if err != nil {
		return nil, nil, err
	}
	store, err := deviceuse.Open(cfg.JournalDirectory, cfg.Scope)
	if err != nil {
		return nil, nil, err
	}
	group := ""
	if csiLVM.DriverName != csiLVM.OfficialDriverName {
		group = csiLVM.DefaultVolumeGroup
	}
	domain := &deviceuse.Domain{Scope: cfg.Scope, Store: store, Resolver: deviceuse.KubernetesResolver{
		Scope: cfg.Scope, Client: client, Native: deviceuse.NativeLVM(manager, csiLVM.DriverName, group)}}
	cert, err := tls.LoadX509KeyPair(cfg.ServerCertFile, cfg.ServerKeyFile)
	if err != nil {
		return nil, nil, err
	}
	ca, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("invalid device-use client roots")
	}
	server, err := deviceuse.NewServer(domain, cert, roots, cfg.Grants)
	if err != nil {
		return nil, nil, err
	}
	return domain, &useListener{address: cfg.Endpoint, server: server}, nil
}

func useReader(path string) (kubernetes.Interface, error) {
	kc, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(kc.Clusters) != 1 || len(kc.AuthInfos) != 1 || len(kc.Contexts) != 1 || kc.CurrentContext == "" {
		return nil, errors.New("reader needs one explicit identity and context")
	}
	for _, c := range kc.Clusters {
		u, err := url.Parse(c.Server)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
			c.InsecureSkipTLSVerify || c.ProxyURL != "" || len(c.CertificateAuthorityData) == 0 || c.CertificateAuthority != "" {
			return nil, errors.New("reader needs HTTPS with embedded CA and no proxy")
		}
	}
	for _, a := range kc.AuthInfos {
		if a.Exec != nil || a.AuthProvider != nil || a.Impersonate != "" || a.ImpersonateUID != "" ||
			len(a.ImpersonateGroups) != 0 || len(a.ImpersonateUserExtra) != 0 || a.Username != "" ||
			a.Password != "" || a.ClientCertificate != "" || a.ClientKey != "" ||
			(a.Token == "" && a.TokenFile == "" && (len(a.ClientCertificateData) == 0 || len(a.ClientKeyData) == 0)) {
			return nil, errors.New("reader needs dedicated token or embedded mTLS")
		}
		if a.TokenFile != "" {
			if filepath.Dir(a.TokenFile) != filepath.Dir(path) {
				return nil, errors.New("reader token must share its protected configuration directory")
			}
			if err := protectedFile(a.TokenFile); err != nil {
				return nil, err
			}
		}
	}
	rc, err := clientcmd.NewDefaultClientConfig(*kc, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	rc.Timeout, rc.QPS, rc.Burst = 10*time.Second, 2, 4
	rc.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	return kubernetes.NewForConfig(rc)
}

type useListener struct {
	address string
	server  *grpc.Server
}

func (*useListener) NeedLeaderElection() bool { return false }
func (l *useListener) Start(ctx context.Context) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", l.address)
	if err != nil {
		return err
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, l.server.Stop)
	defer stop()
	err = l.server.Serve(listener)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
