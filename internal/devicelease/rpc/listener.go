// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type Grant struct {
	URI           string   `json:"uri"`
	NodeID        string   `json:"nodeID"`
	Role          string   `json:"role"`
	Composites    []string `json:"composites"`
	AllComposites bool     `json:"allComposites"`
}
type policy struct {
	Provider     string  `json:"provider"`
	Grants       []Grant `json:"grants"`
	LocalCustody bool    `json:"localCustody"`
}

type openedPeerKey struct{}

func (p policy) authorize(node string) (grpc.UnaryServerInterceptor, error) {
	if node == "" || p.Provider == "" || len(p.Grants) == 0 {
		return nil, errors.New("missing node, provider or grant")
	}
	grants := make(map[string]Grant, len(p.Grants))
	for _, g := range p.Grants {
		u, err := url.Parse(g.URI)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
			u.String() != g.URI || grants[g.URI].URI != "" || g.NodeID != node ||
			(g.Role != "controller" && g.Role != "node") || (len(g.Composites) == 0) != g.AllComposites {
			return nil, errors.New("invalid or ambiguous provider grant")
		}
		for _, c := range g.Composites {
			if c == "" {
				return nil, errors.New("empty composite in grant")
			}
		}
		grants[g.URI] = g
	}
	// Bound concurrent journal waiters independently of HTTP/2 connections.
	slots := make(chan struct{}, 8)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		caller, _ := peer.FromContext(ctx)
		if caller == nil {
			return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
		}
		t, ok := caller.AuthInfo.(credentials.TLSInfo)
		if !ok || len(t.State.VerifiedChains) == 0 || len(t.State.VerifiedChains[0]) == 0 {
			return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
		}
		chain := t.State.VerifiedChains[0]
		now := time.Now()
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				return nil, status.Error(codes.Unauthenticated, "expired client chain")
			}
		}
		if len(chain[0].URIs) != 1 {
			return nil, denied
		}
		g, ok := grants[chain[0].URIs[0].String()]
		if !ok {
			return nil, denied
		}
		ctx = context.WithValue(ctx, grantKey{}, g)
		switch info.FullMethod {
		case pb.DeviceProvider_GetCapacity_FullMethodName, pb.DeviceProvider_GetLease_FullMethodName:
		case pb.DeviceProvider_Reserve_FullMethodName:
			if g.Role != "controller" {
				return nil, denied
			}
		case pb.DeviceProvider_Acquire_FullMethodName, pb.DeviceProvider_Validate_FullMethodName,
			pb.DeviceProvider_Renew_FullMethodName, pb.DeviceProvider_Release_FullMethodName:
			// Recheck node and composite scope even for a historical replay.
			if err := scopeOwner(ctx, req, g, p.Provider); err != nil {
				return nil, err
			}
			if !p.LocalCustody {
				return nil, blocked
			}
			if info.FullMethod == pb.DeviceProvider_Validate_FullMethodName {
				if len(chain[0].Raw) == 0 || len(t.State.PeerCertificates) == 0 || t.State.PeerCertificates[0] == nil ||
					!bytes.Equal(chain[0].Raw, t.State.PeerCertificates[0].Raw) {
					return nil, status.Error(codes.Unauthenticated, "verified client leaf required")
				}
				ctx = context.WithValue(ctx, openedPeerKey{}, sha256.Sum256(chain[0].Raw))
			}
		default:
			return nil, denied
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.Unavailable, "provider busy; retry")
		}
		return next(ctx, req)
	}, nil
}

type Listener struct {
	address string
	server  *grpc.Server
}

func New(address, directory, node string, db *devicelease.Provider) (*Listener, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(directory, "tls.crt"), filepath.Join(directory, "tls.key"))
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(filepath.Join(directory, "client-ca.crt"))
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid provider client CA")
	}
	f, err := os.Open(filepath.Join(directory, "policy.json"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	var p policy
	if err := d.Decode(&p); err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid trailing policy")
	}
	auth, err := p.authorize(node)
	if err != nil {
		return nil, err
	}
	var authority devicelease.Authority
	if p.LocalCustody {
		c := custodian(filepath.Join(directory, "custody.sock"))
		authority = c
		if db.PoolID() == devicelease.OrdinaryPool {
			authority = ordinaryCustodian{c}
		}
	}
	if db.PoolID() == devicelease.OrdinaryPool && !p.LocalCustody {
		return nil, errors.New("ordinary Block hold requires local custody")
	}
	s := newServer(cert, roots, p.Provider, auth, db, authority)
	return &Listener{address: address, server: s}, nil
}

func newServer(cert tls.Certificate, roots *x509.CertPool, provider string, auth grpc.UnaryServerInterceptor, db *devicelease.Provider, authority devicelease.Authority) *grpc.Server {
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, Certificates: []tls.Certificate{cert},
	})), grpc.UnaryInterceptor(auth), grpc.MaxRecvMsgSize(64<<10), grpc.MaxConcurrentStreams(16))
	pb.RegisterDeviceProviderServer(s, &service{provider: provider, db: db, authority: authority})
	return s
}

func (*Listener) NeedLeaderElection() bool { return false }
func (l *Listener) Start(ctx context.Context) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", l.address)
	if err != nil {
		return fmt.Errorf("provider listen: %w", err)
	}
	defer lis.Close()
	stop := context.AfterFunc(ctx, l.server.Stop)
	defer stop()
	err = l.server.Serve(lis)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
