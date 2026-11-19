// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"net/url"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type media struct {
	mu      sync.Mutex
	volumes []devicelease.Volume
}

func (m *media) Inspect(context.Context) (devicelease.Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return devicelease.Pool{UUID: "vg-uuid", ExtentBytes: 4096, FreeBytes: 8192 - int64(len(m.volumes))*8192, Volumes: slices.Clone(m.volumes)}, nil
}
func (m *media) CreateInactive(_ context.Context, name string, tags []string, extents int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volumes = append(m.volumes, devicelease.Volume{UUID: "lv-uuid", Name: name, Bytes: extents * 4096, Tags: tags, Inactive: true})
	return nil
}

// Only media is mocked. Use the actual journal and generated client over
// HTTP/2 and mTLS; no requests use a synthetic peer attached to a context.
func TestRawCacheAuthenticatedRPC(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := new(media)
	db, err := devicelease.Open(dir, backend)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := db.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	const controller = "spiffe://test/controller/n1"
	const other = "spiffe://test/other/n1"
	const node = "spiffe://test/node/n1"
	p := policy{Provider: "vg-uuid", Grants: []Grant{
		{URI: controller, NodeID: "n1", Role: "controller", Composites: []string{"vol"}},
		{URI: other, NodeID: "n1", Role: "controller", Composites: []string{"other"}},
		{URI: node, NodeID: "n1", Role: "node", Composites: []string{"vol"}},
	}}
	if _, err := p.authorize("n2"); err == nil {
		t.Fatal("accepted policy for another node")
	}
	client := authenticatedClients(t, p, db, nil)
	check := func(err error, want codes.Code) {
		t.Helper()
		if status.Code(err) != want {
			t.Fatalf("got %v; want %s", err, want)
		}
	}
	pool := &pb.Pool{Provider: "vg-uuid", PoolId: devicelease.Group}
	_, err = client("").GetCapacity(ctx, pool)
	check(err, codes.Unavailable) // TLS rejected before gRPC admission
	_, err = client("spiffe://test/node/n2").GetCapacity(ctx, pool)
	check(err, codes.PermissionDenied)
	writer, reader, outsider := client(controller), client(node), client(other)
	for _, bad := range []*pb.Pool{{Provider: "vg-uuid", PoolId: "other"}, {Provider: "other", PoolId: devicelease.Group}} {
		_, err = writer.GetCapacity(ctx, bad)
		check(err, codes.PermissionDenied)
	}
	c, err := writer.GetCapacity(ctx, pool)
	check(err, codes.OK)
	if c.AvailableBytes != 8192 || c.MaxAllocationBytes != 8192 {
		t.Fatal(c)
	}
	req := &pb.ReserveRequest{Pool: pool, CompositeVolumeId: "vol", CapacityBytes: 4097}
	_, err = writer.Reserve(ctx, req)
	check(err, codes.InvalidArgument)
	req.RequestId = "r1"
	r, err := writer.Reserve(ctx, req)
	check(err, codes.OK)
	if r.State != pb.LeaseState_LEASE_STATE_RESERVED || r.NodeId != "" || r.ObservedDevicePath != "" || r.CapacityBytes != 8192 ||
		r.Allocation.StableDeviceId != "lv-uuid" || r.Allocation.DeviceGeneration != 1 || r.Allocation.Key.Provider != pool.Provider {
		t.Fatal(r)
	}
	for _, deniedClient := range []pb.DeviceProviderClient{reader, outsider} {
		_, err = deniedClient.Reserve(ctx, req)
		check(err, codes.PermissionDenied)
	}
	replayed, err := writer.Reserve(ctx, req)
	check(err, codes.OK)
	if !proto.Equal(replayed, r) {
		t.Fatalf("changed reservation on retry: %v", replayed)
	}
	req.CapacityBytes++
	_, err = writer.Reserve(ctx, req)
	check(err, codes.AlreadyExists)
	_, err = outsider.GetLease(ctx, r.Allocation.Key)
	check(err, codes.PermissionDenied)
	_, err = writer.GetLease(ctx, &pb.LeaseKey{Provider: "wrong", LeaseId: r.Allocation.Key.LeaseId})
	check(err, codes.PermissionDenied)
	_, err = writer.GetLease(ctx, &pb.LeaseKey{Provider: pool.Provider, LeaseId: "absent"})
	check(err, codes.NotFound)
	live, err := reader.GetLease(ctx, r.Allocation.Key)
	check(err, codes.OK)
	if !proto.Equal(live, r) {
		t.Fatal(live)
	}
	c, err = reader.GetCapacity(ctx, pool)
	check(err, codes.OK)
	if c.AvailableBytes != 0 || c.MaxAllocationBytes != 0 {
		t.Fatal(c)
	}
	owner := &pb.Owner{Allocation: r.Allocation, NodeId: "n1", OwnerEpoch: 1}
	_, err = reader.Acquire(ctx, &pb.AcquireRequest{Owner: owner, RequestId: "r1"})
	check(err, codes.Unavailable)
	_, err = reader.Validate(ctx, &pb.ValidateRequest{Owner: owner, Opened: &pb.OpenedDevice{StableDeviceId: "lv-uuid"}})
	check(err, codes.Unavailable)
	_, err = reader.Renew(ctx, &pb.RenewRequest{Owner: owner, RequestId: "r1"})
	check(err, codes.Unavailable)
	_, err = writer.Release(ctx, &pb.ReleaseRequest{Allocation: r.Allocation, RequestId: "r1", Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION})
	check(err, codes.Unavailable)
	release := &pb.ReleaseRequest{Allocation: r.Allocation, RequestId: "r1", Mode: pb.ReleaseMode_RELEASE_MODE_NODE, NodeId: "n1", OwnerEpoch: 1, CleanupReceiptId: "caller-assertion"}
	_, err = reader.Release(ctx, release)
	check(err, codes.Unavailable)
	release.NodeId = "n2"
	_, err = reader.Release(ctx, release)
	check(err, codes.PermissionDenied)
	backend.mu.Lock()
	if len(backend.volumes) != 1 {
		t.Fatal("destructive or duplicate effect")
	}
	backend.volumes[0].UUID = "replacement"
	backend.mu.Unlock()
	_, err = writer.GetLease(ctx, r.Allocation.Key)
	check(err, codes.Unavailable)
	_, err = writer.GetCapacity(ctx, pool)
	check(err, codes.Unavailable)
}

func authenticatedClients(t *testing.T, p policy, db *devicelease.Provider, proof devicelease.Authority) func(string) pb.DeviceProviderClient {
	t.Helper()
	auth, err := p.authorize("n1")
	if err != nil {
		t.Fatal(err)
	}
	roots, issue := certificates(t)
	server := newServer(issue(""), roots, p.Provider, auth, db, proof)
	wire := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(wire) }()
	t.Cleanup(server.Stop)
	return func(uri string) pb.DeviceProviderClient {
		t.Helper()
		cfg := &tls.Config{RootCAs: roots, ServerName: "provider.test", MinVersion: tls.VersionTLS13}
		if uri != "" {
			cfg.Certificates = []tls.Certificate{issue(uri)}
		}
		cc, err := grpc.NewClient("passthrough:///provider.test",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return wire.DialContext(ctx) }),
			grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return pb.NewDeviceProviderClient(cc)
	}
}

type handoffMedia struct{ media }

func (m *handoffMedia) Activate(_ context.Context, r devicelease.Reservation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := &m.volumes[0]
	if !v.Inactive || v.UUID != r.StableDeviceID {
		return devicelease.ErrUncertain
	}
	v.Active, v.Inactive, v.Major, v.Minor = true, false, 253, 7
	return nil
}
func (m *handoffMedia) Prepare(context.Context, devicelease.Reservation, devicelease.Volume, bool) error {
	return nil
}
func (m *handoffMedia) Deactivate(context.Context, devicelease.Reservation, devicelease.Volume) error {
	return devicelease.ErrHeld
}
func (m *handoffMedia) ConfirmInactive(context.Context, devicelease.Reservation) error {
	return devicelease.ErrHeld
}

type enrolled struct{ owner devicelease.Owner }

func (a *enrolled) Enrolled(o devicelease.Owner) error {
	if o != a.owner {
		return devicelease.ErrHeld
	}
	return nil
}
func (a *enrolled) Opened(o devicelease.Owner, v devicelease.Volume) error {
	if !v.Open || v.UUID != a.owner.StableDeviceID {
		return devicelease.ErrHeld
	}
	return a.Enrolled(o)
}
func (*enrolled) Closed(devicelease.Owner, string) error { return devicelease.ErrHeld }

func TestScopedCustodyRPC(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	m := new(handoffMedia)
	db, err := devicelease.Open(dir, m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := db.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := db.Reserve(ctx, "reserve", "vol", 4097)
	if err != nil {
		t.Fatal(err)
	}
	alloc := lease(r).Allocation
	o := &pb.Owner{NodeId: "n1", OwnerEpoch: 1, Allocation: alloc}
	proof := &enrolled{owner: devicelease.Owner{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID,
		StableDeviceID: r.StableDeviceID, ResourceID: alloc.ResourceId, Generation: r.Generation, NodeID: "n1", Epoch: 1}}
	const node = "spiffe://test/custody/n1"
	const other = "spiffe://test/other/n1"
	p := policy{Provider: r.PoolUUID, Grants: []Grant{
		{URI: node, Role: "node", NodeID: "n1", Composites: []string{"vol"}},
		{URI: other, Role: "node", NodeID: "n1", Composites: []string{"other"}},
	}}
	q := &pb.AcquireRequest{Owner: o, RequestId: "acquire"}
	// Even a fully capable backend and valid proof do not turn custody on.
	if _, err := authenticatedClients(t, p, db, proof)(node).Acquire(ctx, q); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	p.LocalCustody = true
	client := authenticatedClients(t, p, db, proof)
	c, unauthorized := client(node), client(other)
	got, err := c.Acquire(ctx, q)
	if err != nil || got.State != pb.LeaseState_LEASE_STATE_ACQUIRED || got.NodeId != "n1" {
		t.Fatal(got, err)
	}
	if _, err := unauthorized.Acquire(ctx, q); status.Code(err) != codes.PermissionDenied {
		t.Fatal("out-of-scope replay", err)
	}
	wrong := proto.Clone(q).(*pb.AcquireRequest)
	wrong.Owner.NodeId = "n2"
	if _, err := c.Acquire(ctx, wrong); status.Code(err) != codes.PermissionDenied {
		t.Fatal("wrong node", err)
	}
	replay, err := c.Acquire(ctx, q)
	if err != nil || !proto.Equal(got, replay) {
		t.Fatal(replay, err)
	}
	m.mu.Lock()
	m.volumes[0].Open = true
	m.mu.Unlock()
	check := &pb.ValidateRequest{Owner: o, Opened: &pb.OpenedDevice{StableDeviceId: r.StableDeviceID, DeviceGeneration: r.Generation, CapacityBytes: 8192, ObservedMajor: 253, ObservedMinor: 7}}
	if _, err := c.Validate(ctx, check); err != nil {
		t.Fatal(err)
	}
	if _, err := unauthorized.Validate(ctx, check); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func TestScopeOwnerRejectsUnknownRequest(t *testing.T) {
	g := Grant{Role: "node", NodeID: "n1", AllComposites: true}
	ctx := context.WithValue(t.Context(), grantKey{}, g)
	if err := scopeOwner(ctx, &pb.Capacity{}, g, "vg"); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func certificates(t *testing.T) (*x509.CertPool, func(string) tls.Certificate) {
	t.Helper()
	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	base := x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	ca := base
	ca.IsCA, ca.BasicConstraintsValid, ca.KeyUsage = true, true, x509.KeyUsageCertSign
	der, err := x509.CreateCertificate(rand.Reader, &ca, &ca, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	serial := int64(1)
	return pool, func(uri string) tls.Certificate {
		t.Helper()
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := base
		serial++
		leaf.SerialNumber = big.NewInt(serial)
		leaf.KeyUsage = x509.KeyUsageDigitalSignature
		if uri == "" {
			leaf.DNSNames = []string{"provider.test"}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			u, err := url.Parse(uri)
			if err != nil {
				t.Fatal(err)
			}
			leaf.URIs = []*url.URL{u}
			leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, &leaf, root, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
}
