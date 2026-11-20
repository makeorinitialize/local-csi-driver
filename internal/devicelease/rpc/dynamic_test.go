// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type capturedPeerAuthority struct {
	*enrolled
	seen chan [sha256.Size]byte
}

func (a *capturedPeerAuthority) ForPeer(peer [sha256.Size]byte) devicelease.Authority {
	a.seen <- peer
	return a.enrolled
}

func TestHostAuthorityReceivesVerifiedAcquireLeaf(t *testing.T) {
	cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
		CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
		Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
	db, err := devicelease.OpenDynamic(filepath.Join(t.TempDir(), "peer"), cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", "one", cfg.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	o := &pb.Owner{Allocation: lease(r).Allocation, NodeId: "n1", OwnerEpoch: 1}
	expected, err := owner(o)
	if err != nil {
		t.Fatal(err)
	}
	a := &capturedPeerAuthority{enrolled: &enrolled{owner: expected}, seen: make(chan [sha256.Size]byte, 1)}
	const node = "spiffe://test/node/n1"
	clients, leaves := authenticatedTLSClients(t, policy{Provider: cfg.PoolUUID, LocalCustody: true,
		Grants: []Grant{{URI: node, NodeID: "n1", Role: "node", Composites: []string{"one"}}}}, db, a)
	c := clients(node)
	if _, err := c.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire", Owner: o}); err != nil {
		t.Fatal(err)
	}
	got := <-a.seen
	if hex.EncodeToString(got[:]) != (*leaves)[0] {
		t.Fatal("member peer was not independently obtained from verified mTLS")
	}
}

func (canaryObservation) DiscoverOrdinary(_ context.Context, e devicelease.Enrollment) (devicelease.Enrollment, error) {
	e.LVUUID = "observed-lv-uuid"
	return e, nil
}

type canarySource struct {
	mu       sync.Mutex
	c        devicelease.Candidate
	deleting bool
}

func (r *canarySource) Resolve(context.Context, string, int64) (devicelease.Candidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleting {
		return devicelease.Candidate{}, devicelease.ErrUncertain
	}
	return r.c, nil
}
func TestDynamicRPCReservedReceiptBeforeAttachment(t *testing.T) {
	cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
		CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
		Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
	path := filepath.Join(t.TempDir(), "online")
	db, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	const ctl, node, other = "spiffe://test/controller/n1", "spiffe://test/node/n1", "spiffe://test/other/n1"
	policy := policy{Provider: cfg.PoolUUID, LocalCustody: true, Grants: []Grant{
		{URI: ctl, NodeID: cfg.NodeName, Role: "controller", Composites: []string{"one"}},
		{URI: node, NodeID: cfg.NodeName, Role: "node", Composites: []string{"one"}},
		{URI: other, NodeID: cfg.NodeName, Role: "controller", Composites: []string{"other"}},
	}}
	clients := authenticatedClients(t, policy, db, new(enrolled))
	controller, consumer := clients(ctl), clients(node)
	if _, err := controller.GetLease(t.Context(), &pb.LeaseKey{Provider: cfg.PoolUUID, LeaseId: "unknown"}); status.Code(err) != codes.Unavailable {
		t.Fatal("unknown key reported authoritative absence", err)
	}
	q := &pb.ReserveRequest{RequestId: "reserve", CompositeVolumeId: "one", CapacityBytes: uint64(cfg.Bytes),
		Pool: &pb.Pool{Provider: cfg.PoolUUID, PoolId: devicelease.OrdinaryPool}}
	if _, err := consumer.Reserve(t.Context(), q); status.Code(err) != codes.PermissionDenied {
		t.Fatal("node principal enrolled media", err)
	}
	q.RequestId = "not-the-spec-request"
	if _, err := controller.Reserve(t.Context(), q); status.Code(err) != codes.AlreadyExists {
		t.Fatal("unbound source request admitted", err)
	}
	q.RequestId = "reserve"
	reserved, err := controller.Reserve(t.Context(), q)
	if err != nil || reserved.GetState() != pb.LeaseState_LEASE_STATE_RESERVED || reserved.ObservedDevicePath != "" ||
		reserved.Allocation.StableDeviceId != "observed-lv-uuid" || reserved.Allocation.Key.Provider != cfg.PoolUUID ||
		reserved.PoolId != devicelease.OrdinaryPool {
		t.Fatal("Reserve did not return real pre-attach identity", reserved, err)
	}
	got, err := controller.GetLease(t.Context(), reserved.Allocation.Key)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("pre-attach receipt not discoverable", got, err)
	}
	// DeleteHandle can begin deletion before recovering a lost Reserve reply.
	source.mu.Lock()
	source.deleting = true
	source.mu.Unlock()
	got, err = controller.Reserve(t.Context(), q)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("deletion stranded committed receipt", got, err)
	}
	got, err = controller.GetLease(t.Context(), reserved.Allocation.Key)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("deleting source lost committed receipt", got, err)
	}
	if _, err := consumer.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire",
		Owner: &pb.Owner{Allocation: reserved.Allocation, NodeId: cfg.NodeName, OwnerEpoch: 1}}); status.Code(err) != codes.Unavailable {
		t.Fatal("receipt recovery enabled node access to deleting source", err)
	}
	source.mu.Lock()
	source.c.CompositeUID = "replacement"
	source.mu.Unlock()
	if got, err := controller.GetLease(t.Context(), reserved.Allocation.Key); err != nil || !proto.Equal(got, reserved) {
		t.Fatal("observation did not preserve original committed identity", got, err)
	}
	closeRequest := &pb.ReleaseRequest{RequestId: "release-reserve-0", Allocation: reserved.Allocation,
		Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION}
	if _, err := consumer.Release(t.Context(), closeRequest); status.Code(err) != codes.PermissionDenied {
		t.Fatal("node principal closed allocation", err)
	}
	for _, mutate := range []func(*pb.ReleaseRequest){
		func(q *pb.ReleaseRequest) { q.OwnerEpoch = 1 },
		func(q *pb.ReleaseRequest) { q.Allocation.DeviceGeneration++ },
		func(q *pb.ReleaseRequest) { q.Allocation.StableDeviceId += "-changed" },
		func(q *pb.ReleaseRequest) { q.Allocation.ResourceId = "/dev/" + q.Allocation.ResourceId },
		func(q *pb.ReleaseRequest) { q.Allocation.Key.LeaseId += "-changed" },
	} {
		wrong := proto.Clone(closeRequest).(*pb.ReleaseRequest)
		mutate(wrong)
		if _, err := controller.Release(t.Context(), wrong); status.Code(err) != codes.Aborted {
			t.Fatal("inexact closure accepted", wrong, err)
		}
	}
	conflict := proto.Clone(closeRequest).(*pb.ReleaseRequest)
	conflict.RequestId = "reserve"
	if _, err := controller.Release(t.Context(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatal("closure overwrote Reserve request history", err)
	}
	if _, err := controller.Release(t.Context(), closeRequest); err != nil {
		t.Fatal("committed never-acquired allocation could not close", err)
	}
	// Restart after a lost close reply. Source identity is no longer available;
	// only the exact durable tombstone can acknowledge this key as closed.
	reopened, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	restarted := authenticatedClients(t, policy, reopened, new(enrolled))
	controller, consumer = restarted(ctl), restarted(node)
	if _, err := controller.Release(t.Context(), closeRequest); err != nil {
		t.Fatal("exact closure replay depended on live source", err)
	}
	if _, err := controller.GetLease(t.Context(), reserved.Allocation.Key); status.Code(err) != codes.NotFound {
		t.Fatal("closed key lost authoritative absence", err)
	}
	if _, err := restarted(other).GetLease(t.Context(), reserved.Allocation.Key); status.Code(err) != codes.PermissionDenied {
		t.Fatal("closed-key observation bypassed composite authorization", err)
	}
	if _, err := controller.GetLease(t.Context(), &pb.LeaseKey{Provider: cfg.PoolUUID, LeaseId: "unknown"}); status.Code(err) != codes.Unavailable {
		t.Fatal("unknown key confused with closed key", err)
	}
	if _, err := controller.Reserve(t.Context(), q); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("late Reserve replay reopened closed authorization", err)
	}
	if _, err := consumer.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire",
		Owner: &pb.Owner{Allocation: reserved.Allocation, NodeId: cfg.NodeName, OwnerEpoch: 1}}); status.Code(err) != codes.Aborted {
		t.Fatal("late Acquire reopened closed authorization", err)
	}
	conflict = proto.Clone(closeRequest).(*pb.ReleaseRequest)
	conflict.OwnerEpoch++
	if _, err := controller.Release(t.Context(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatal("closure replay changed committed epoch", err)
	}
}
