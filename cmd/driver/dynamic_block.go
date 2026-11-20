// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"local-csi-driver/internal/devicelease"
	"local-csi-driver/internal/devicelease/lvbackend"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

const enrollmentConfigDirectory = "/etc/acs-dmstack-enrollment"

func protectedFile(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 || st.Sys().(*syscall.Stat_t).Uid != 0 {
		return errors.New("enrollment configuration must be root-owned and not group/world writable")
	}
	return nil
}

func ordinaryProvider(node string, manager lvmMgr.Manager) (*devicelease.Provider, error) {
	b := lvbackend.Backend{Manager: manager}
	mode := os.Getenv("ACS_DYNAMIC_BLOCK")
	if mode == "" {
		return devicelease.OpenOrdinary(ordinaryDirectory, node, b)
	}
	if mode != "1" || os.Getenv("ACS_RAW_CACHE") != "1" {
		return nil, errors.New("dynamic Block enrollment requires both explicit opt-ins")
	}
	configPath := filepath.Join(enrollmentConfigDirectory, "domain.json")
	kubeconfigPath := filepath.Join(enrollmentConfigDirectory, "reader.kubeconfig")
	for _, path := range []string{configPath, kubeconfigPath} {
		if err := protectedFile(path); err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	domain, err := devicelease.DecodeDynamicConfig(data)
	if err != nil || domain.NodeName != node {
		return nil, errors.New("invalid or wrong-node dynamic enrollment domain")
	}
	// No in-cluster fallback, ambient credentials, exec plugins, auth plugins,
	// insecure TLS, or proxy routing. The dedicated reader is not the manager
	// client and is never exposed to workloads or the requesting facade.
	kc, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return nil, err
	}
	if len(kc.Clusters) != 1 || len(kc.AuthInfos) != 1 || len(kc.Contexts) != 1 || kc.CurrentContext == "" {
		return nil, errors.New("reader kubeconfig must contain exactly one explicit identity and context")
	}
	for _, c := range kc.Clusters {
		u, err := url.Parse(c.Server)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
			c.InsecureSkipTLSVerify || c.ProxyURL != "" || len(c.CertificateAuthorityData) == 0 || c.CertificateAuthority != "" {
			return nil, errors.New("reader requires HTTPS with embedded pinned CA and no proxy")
		}
	}
	for _, a := range kc.AuthInfos {
		if a.Exec != nil || a.AuthProvider != nil || a.Impersonate != "" || a.ImpersonateUID != "" ||
			len(a.ImpersonateGroups) != 0 || len(a.ImpersonateUserExtra) != 0 || a.Username != "" ||
			a.Password != "" || a.ClientCertificate != "" || a.ClientKey != "" ||
			(a.Token == "" && a.TokenFile == "" && (len(a.ClientCertificateData) == 0 || len(a.ClientKeyData) == 0)) {
			return nil, errors.New("reader requires dedicated token or embedded mTLS credentials")
		}
		if a.TokenFile != "" {
			if filepath.Dir(a.TokenFile) != enrollmentConfigDirectory {
				return nil, errors.New("reader token must be in the protected enrollment mount")
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
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	reader, err := client.New(rc, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return devicelease.OpenDynamic(ordinaryDirectory, domain, devicelease.KubernetesSource{Reader: reader, Config: domain}, b)
}
