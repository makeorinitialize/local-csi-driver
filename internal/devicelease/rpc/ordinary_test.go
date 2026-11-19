// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type canaryObservation struct{}

func (canaryObservation) BlockTarget(string) (*devicelease.BlockTarget, error) { return nil, nil }

func (canaryObservation) InspectOrdinary(_ context.Context, e devicelease.Enrollment) (devicelease.Volume, error) {
	_, name, _ := strings.Cut(e.VolumeHandle, "#")
	return devicelease.Volume{Name: name, UUID: e.LVUUID, Bytes: e.Bytes, Active: true, Major: 253, Minor: 1}, nil
}
func (canaryObservation) WithExclusive(_ devicelease.Enrollment, _ devicelease.Volume, fn func() error) error {
	return fn()
}

func TestRPCResolveEnrolledOrdinaryLV(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "ordinary")
	e := devicelease.Enrollment{NodeID: "n1", CompositeID: "one", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc",
		VGUUID: "vg-one", LVUUID: "lv-one", VolumeMode: "Block", Bytes: 8192, Generation: 7}
	b := canaryObservation{}
	pin, err := devicelease.PinEnrollment(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := devicelease.InitializeOrdinary(ctx, path, e, b); err == nil {
		t.Fatal("enrolled during running unguarded instance")
	}
	pin()
	if err := devicelease.InitializeOrdinary(ctx, path, e, b); err != nil {
		t.Fatal(err)
	}
	p, err := devicelease.OpenOrdinary(path, "n1", b)
	if err != nil {
		t.Fatal(err)
	}
	const ctl = "spiffe://test/controller/n1"
	const node = "spiffe://test/node/n1"
	policy := policy{Provider: e.VGUUID, LocalCustody: true, Grants: []Grant{
		{URI: ctl, NodeID: "n1", Role: "controller", Composites: []string{"one"}},
		{URI: node, NodeID: "n1", Role: "node", Composites: []string{"one"}},
	}}
	proof := new(enrolled)
	clients := authenticatedClients(t, policy, p, proof)
	writer, reader := clients(ctl), clients(node)
	q := &pb.ReserveRequest{Pool: &pb.Pool{Provider: e.VGUUID, PoolId: devicelease.OrdinaryPool}, RequestId: "resolve", CompositeVolumeId: "one", CapacityBytes: uint64(e.Bytes)}
	if _, err := reader.Reserve(ctx, q); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	lease, err := writer.Reserve(ctx, q)
	if err != nil || lease.PoolId != devicelease.OrdinaryPool || lease.Allocation.ResourceId != strings.Replace(e.VolumeHandle, "#", "/", 1) ||
		lease.Allocation.StableDeviceId != e.LVUUID || lease.Allocation.DeviceGeneration != 7 || lease.CapacityBytes != 8192 {
		t.Fatal(lease, err)
	}
	o := &pb.Owner{NodeId: "n1", OwnerEpoch: 1, Allocation: lease.Allocation}
	proof.owner, err = owner(o)
	if err != nil {
		t.Fatal(err)
	}
	acq, err := reader.Acquire(ctx, &pb.AcquireRequest{Owner: o, RequestId: "acquire"})
	if err != nil || acq.State != pb.LeaseState_LEASE_STATE_ACQUIRED || acq.ObservedDevicePath != "/dev/"+lease.Allocation.ResourceId {
		t.Fatal(acq, err)
	}
}
