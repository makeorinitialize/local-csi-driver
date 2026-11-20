// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"context"
	"crypto/sha256"

	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

func (s *service) authorityFor(ctx context.Context) devicelease.Authority {
	if member, ok := s.authority.(interface {
		ForPeer([sha256.Size]byte) devicelease.Authority
	}); ok {
		peer, ok := ctx.Value(openedPeerKey{}).([sha256.Size]byte)
		if !ok {
			return nil
		}
		return member.ForPeer(peer)
	}
	return s.authority
}

func current(m proto.Message) bool {
	return m.ProtoReflect().IsValid() && len(m.ProtoReflect().GetUnknown()) == 0
}
func owner(o *pb.Owner) (devicelease.Owner, error) {
	a := o.GetAllocation()
	k := a.GetKey()
	if !current(o) || !current(a) || !current(k) || k.Provider == "" || k.LeaseId == "" || a.CompositeVolumeId == "" || a.ResourceId == "" ||
		a.StableDeviceId == "" || a.DeviceGeneration == 0 || o.NodeId == "" || o.OwnerEpoch == 0 {
		return devicelease.Owner{}, invalid
	}
	return devicelease.Owner{PoolUUID: k.Provider, LeaseID: k.LeaseId, CompositeID: a.CompositeVolumeId, ResourceID: a.ResourceId,
		StableDeviceID: a.StableDeviceId, Generation: a.DeviceGeneration, NodeID: o.NodeId, Epoch: o.OwnerEpoch}, nil
}
func (s *service) Acquire(ctx context.Context, q *pb.AcquireRequest) (*pb.Lease, error) {
	if !current(q) || q.RequestId == "" {
		return nil, invalid
	}
	o, err := owner(q.Owner)
	if err != nil {
		return nil, err
	}
	r, err := s.db.Acquire(ctx, s.authorityFor(ctx), q.RequestId, o, q.PreviousOwnerEpoch)
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(r), nil
}
func (s *service) Renew(ctx context.Context, q *pb.RenewRequest) (*pb.Lease, error) {
	if !current(q) || q.RequestId == "" {
		return nil, invalid
	}
	o, err := owner(q.Owner)
	if err != nil {
		return nil, err
	}
	r, err := s.db.Renew(ctx, s.authorityFor(ctx), q.RequestId, o)
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(r), nil
}
func (s *service) Validate(ctx context.Context, q *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	if !current(q) || !current(q.Opened) {
		return nil, invalid
	}
	o, err := owner(q.Owner)
	if err != nil {
		return nil, err
	}
	d := q.Opened
	if d.StableDeviceId != o.StableDeviceID || d.DeviceGeneration != o.Generation {
		return nil, rpcError(devicelease.ErrStale)
	}
	if err := s.db.ValidateOpen(ctx, s.authorityFor(ctx), o, d.CapacityBytes, d.ObservedMajor, d.ObservedMinor); err != nil {
		return nil, rpcError(err)
	}
	return &pb.ValidateResponse{}, nil
}
func (s *service) Release(ctx context.Context, q *pb.ReleaseRequest) (*pb.ReleaseAck, error) {
	if !current(q) || q.RequestId == "" {
		return nil, invalid
	}
	if q.Mode == pb.ReleaseMode_RELEASE_MODE_ALLOCATION {
		a := q.Allocation
		k := a.GetKey()
		if !current(a) || !current(k) || q.NodeId != "" || q.CleanupReceiptId != "" ||
			k.Provider == "" || k.LeaseId == "" || a.CompositeVolumeId == "" || a.ResourceId == "" ||
			a.StableDeviceId == "" || a.DeviceGeneration == 0 {
			return nil, invalid
		}
		if err := s.db.ReleaseAllocation(ctx, q.RequestId, devicelease.Allocation{
			PoolUUID: k.Provider, CompositeID: a.CompositeVolumeId, LeaseID: k.LeaseId,
			ResourceID: a.ResourceId, StableDeviceID: a.StableDeviceId, Generation: a.DeviceGeneration,
		}, q.OwnerEpoch); err != nil {
			return nil, rpcError(err)
		}
		return &pb.ReleaseAck{}, nil
	}
	if q.Mode != pb.ReleaseMode_RELEASE_MODE_NODE {
		return nil, blocked
	}
	if q.CleanupReceiptId == "" {
		return nil, invalid
	}
	o, err := owner(&pb.Owner{Allocation: q.Allocation, NodeId: q.NodeId, OwnerEpoch: q.OwnerEpoch})
	if err != nil {
		return nil, err
	}
	if err := s.db.ReleaseNode(ctx, s.authorityFor(ctx), q.RequestId, o, q.CleanupReceiptId); err != nil {
		return nil, rpcError(err)
	}
	return &pb.ReleaseAck{}, nil
}
