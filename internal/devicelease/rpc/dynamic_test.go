// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

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
func (r *canarySource) ResolveCommitted(context.Context, string, int64) (devicelease.Candidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	const ctl, node = "spiffe://test/controller/n1", "spiffe://test/node/n1"
	policy := policy{Provider: cfg.PoolUUID, LocalCustody: true, Grants: []Grant{
		{URI: ctl, NodeID: cfg.NodeName, Role: "controller", Composites: []string{"one"}},
		{URI: node, NodeID: cfg.NodeName, Role: "node", Composites: []string{"one"}},
	}}
	clients := authenticatedClients(t, policy, db, new(enrolled))
	controller, consumer := clients(ctl), clients(node)
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
	if _, err := controller.GetLease(t.Context(), reserved.Allocation.Key); status.Code(err) != codes.Unavailable {
		t.Fatal("receipt recovery adopted replaced source", err)
	}
}
