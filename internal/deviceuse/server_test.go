// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	pb "local-csi-driver/internal/deviceuse/usev1"
)

func TestNodeCertificateAndPVGrantCannotBeSelfAsserted(t *testing.T) {
	d, _, _, id := fixture(t)
	auth, err := Authorize(d.Scope, []Grant{
		{URI: "spiffe://test/node", Node: "node", Namespace: "sources", PVNames: []string{"pv"}},
		{URI: "spiffe://test/reader", Node: "node", Namespace: "sources", PVNames: []string{"pv"}, ReadOnly: true},
	})
	requireCode(t, err, codes.OK)
	call := func(uri, method string, req any) (any, error) {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{URIs: []*url.URL{u}, Raw: []byte(uri),
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
		ctx := peer.NewContext(t.Context(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			VerifiedChains: [][]*x509.Certificate{{leaf}}, PeerCertificates: []*x509.Certificate{leaf}}}})
		return auth(ctx, req, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, req any) (any, error) {
			switch q := req.(type) {
			case *pb.AcquireUseRequest:
				return d.AcquireUse(ctx, q)
			case *pb.ReleaseUseRequest:
				return d.ReleaseUse(ctx, q)
			default:
				return d.GetUse(ctx, req.(*pb.GetUseRequest))
			}
		})
	}
	_, err = call("spiffe://test/node", pb.DeviceUse_AcquireUse_FullMethodName, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	for _, uri := range []string{"spiffe://test/foreign", "spiffe://test/reader"} {
		_, err = call(uri, pb.DeviceUse_ReleaseUse_FullMethodName, &pb.ReleaseUseRequest{Identity: id})
		requireCode(t, err, codes.PermissionDenied)
	}
	other := proto.CloneOf(id)
	other.Source.PvName = "foreign-pv"
	_, err = call("spiffe://test/node", pb.DeviceUse_ReleaseUse_FullMethodName, &pb.ReleaseUseRequest{Identity: other})
	requireCode(t, err, codes.PermissionDenied)
	other = proto.CloneOf(id)
	other.NodeId = "other-node"
	_, err = call("spiffe://test/node", pb.DeviceUse_ReleaseUse_FullMethodName, &pb.ReleaseUseRequest{Identity: other})
	requireCode(t, err, codes.InvalidArgument)
	out, err := call("spiffe://test/reader", pb.DeviceUse_GetUse_FullMethodName, &pb.GetUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if out.(*pb.Use).State != pb.UseState_USE_STATE_ACTIVE {
		t.Fatal("unauthorized attempts changed durable use")
	}
	_, err = auth(t.Context(), &pb.GetUseRequest{Identity: id}, &grpc.UnaryServerInfo{FullMethod: pb.DeviceUse_GetUse_FullMethodName},
		func(context.Context, any) (any, error) { t.Fatal("unverified client reached handler"); return nil, nil })
	requireCode(t, err, codes.Unauthenticated)
}
