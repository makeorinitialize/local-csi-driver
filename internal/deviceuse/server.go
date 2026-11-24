// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation"

	pb "local-csi-driver/internal/deviceuse/usev1"
)

type Grant struct {
	URI       string   `json:"uri"`
	Node      string   `json:"node"`
	Namespace string   `json:"namespace"`
	PVNames   []string `json:"pvNames"`
	ReadOnly  bool     `json:"readOnly,omitempty"`
}

func Authorize(scope Scope, input []Grant) (grpc.UnaryServerInterceptor, error) {
	grants := map[string]Grant{}
	for _, g := range input {
		u, err := url.Parse(g.URI)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.User != nil || u.RawQuery != "" ||
			u.Fragment != "" || u.String() != g.URI || grants[g.URI].URI != "" || g.Node != scope.Node ||
			g.Namespace != scope.Namespace || len(g.PVNames) == 0 || slices.Contains(g.PVNames, "") {
			return nil, status.Error(codes.InvalidArgument, "exact independent node/namespace/PV certificate grants required")
		}
		for _, name := range g.PVNames {
			if len(validation.IsDNS1123Subdomain(name)) != 0 {
				return nil, status.Error(codes.InvalidArgument, "exact PV names required in certificate grant")
			}
		}
		grants[g.URI] = g
	}
	if !scope.Valid() || len(grants) == 0 {
		return nil, status.Error(codes.InvalidArgument, "use scope and certificate grants required")
	}
	slots := make(chan struct{}, 8)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		caller, _ := peer.FromContext(ctx)
		if caller == nil {
			return nil, status.Error(codes.Unauthenticated, "verified node certificate required")
		}
		t, ok := caller.AuthInfo.(credentials.TLSInfo)
		if !ok || len(t.State.VerifiedChains) == 0 || len(t.State.VerifiedChains[0]) == 0 ||
			len(t.State.PeerCertificates) == 0 || t.State.PeerCertificates[0] == nil {
			return nil, status.Error(codes.Unauthenticated, "verified node certificate required")
		}
		chain := t.State.VerifiedChains[0]
		for _, cert := range chain {
			if cert == nil || time.Now().Before(cert.NotBefore) || !time.Now().Before(cert.NotAfter) {
				return nil, status.Error(codes.Unauthenticated, "client certificate chain is not currently valid")
			}
		}
		leaf := chain[0]
		if len(leaf.Raw) == 0 || !bytes.Equal(leaf.Raw, t.State.PeerCertificates[0].Raw) || len(leaf.URIs) != 1 {
			return nil, status.Error(codes.Unauthenticated, "exact verified client leaf required")
		}
		g, ok := grants[leaf.URIs[0].String()]
		if !ok {
			return nil, status.Error(codes.PermissionDenied, "node certificate has no independent use grant")
		}
		var id *pb.UseIdentity
		switch q := req.(type) {
		case *pb.AcquireUseRequest:
			if info.FullMethod != pb.DeviceUse_AcquireUse_FullMethodName || g.ReadOnly {
				return nil, status.Error(codes.PermissionDenied, "use mutation not permitted")
			}
			id = q.GetIdentity()
		case *pb.GetUseRequest:
			if info.FullMethod != pb.DeviceUse_GetUse_FullMethodName {
				return nil, status.Error(codes.PermissionDenied, "unknown use method")
			}
			id = q.GetIdentity()
		case *pb.ReleaseUseRequest:
			if info.FullMethod != pb.DeviceUse_ReleaseUse_FullMethodName || g.ReadOnly {
				return nil, status.Error(codes.PermissionDenied, "use mutation not permitted")
			}
			id = q.GetIdentity()
		default:
			return nil, status.Error(codes.PermissionDenied, "unknown use method")
		}
		if err := validateIdentity(scope, id); err != nil {
			return nil, err
		}
		if id.NodeId != g.Node || !slices.Contains(g.PVNames, id.Source.PvName) {
			return nil, status.Error(codes.PermissionDenied, "source or node outside independent certificate grant")
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.Unavailable, "use service busy")
		}
		return next(ctx, req)
	}, nil
}

func NewServer(domain *Domain, cert tls.Certificate, roots *x509.CertPool, grants []Grant) (*grpc.Server, error) {
	if domain == nil || domain.Store == nil || domain.Resolver == nil || roots == nil || len(cert.Certificate) == 0 {
		return nil, status.Error(codes.InvalidArgument, "use domain and mutual TLS required")
	}
	auth, err := Authorize(domain.Scope, grants)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: roots, Certificates: []tls.Certificate{cert},
	})), grpc.UnaryInterceptor(auth), grpc.MaxRecvMsgSize(64<<10), grpc.MaxConcurrentStreams(16))
	pb.RegisterDeviceUseServer(server, domain)
	return server, nil
}
