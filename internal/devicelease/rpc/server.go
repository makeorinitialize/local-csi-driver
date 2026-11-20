// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Package rpc serves only the node-local reservation subset of DeviceProvider v1.
package rpc

import (
	"context"
	"errors"
	"math"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type service struct {
	pb.UnimplementedDeviceProviderServer
	db        *devicelease.Provider
	provider  string // immutable local VG UUID; never a routing alias
	authority devicelease.Authority
}

type grantKey struct{}

func permitted(ctx context.Context, composite string) bool {
	g, ok := ctx.Value(grantKey{}).(Grant)
	return ok && (g.AllComposites || slices.Contains(g.Composites, composite))
}

var (
	denied  = status.Error(codes.PermissionDenied, "outside authorized scope")
	invalid = status.Error(codes.InvalidArgument, "invalid DeviceProvider v1 request")
	blocked = status.Error(codes.Unavailable, "independent node access and teardown authority unavailable")
)

func scopeOwner(ctx context.Context, req any, g Grant, provider string) error {
	var a *pb.Allocation
	var o *pb.Owner
	switch r := req.(type) {
	case *pb.AcquireRequest:
		o = r.GetOwner()
	case *pb.ValidateRequest:
		o = r.GetOwner()
	case *pb.RenewRequest:
		o = r.GetOwner()
	case *pb.ReleaseRequest:
		switch r.GetMode() {
		case pb.ReleaseMode_RELEASE_MODE_NODE:
			if g.Role != "node" || r.NodeId != g.NodeID {
				return denied
			}
		case pb.ReleaseMode_RELEASE_MODE_ALLOCATION:
			if g.Role != "controller" || r.NodeId != "" {
				return denied
			}
		default:
			return invalid
		}
		a = r.Allocation
	default:
		return denied
	}
	if o != nil {
		if g.Role != "node" || o.NodeId != g.NodeID {
			return denied
		}
		a = o.Allocation
	}
	if a.GetKey().GetProvider() != provider || !permitted(ctx, a.GetCompositeVolumeId()) {
		return denied
	}
	return nil
}

func (s *service) pool(p *pb.Pool) error {
	if p.GetProvider() == "" || p.GetPoolId() == "" || len(p.ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	if p.Provider != s.provider || p.PoolId != s.db.PoolID() {
		return denied
	}
	return nil
}

func (s *service) GetCapacity(ctx context.Context, p *pb.Pool) (*pb.Capacity, error) {
	if err := s.pool(p); err != nil {
		return nil, err
	}
	free, err := s.db.Capacity(ctx, s.provider)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.Capacity{AvailableBytes: uint64(free), MaxAllocationBytes: uint64(free)}, nil
}

func (s *service) Reserve(ctx context.Context, req *pb.ReserveRequest) (*pb.Lease, error) {
	if err := s.pool(req.GetPool()); err != nil {
		return nil, err
	}
	if req.RequestId == "" || req.CompositeVolumeId == "" || req.CapacityBytes == 0 || req.CapacityBytes > math.MaxInt64 ||
		len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, invalid
	}
	if !permitted(ctx, req.CompositeVolumeId) {
		return nil, denied
	}
	r, err := s.db.ReserveInPool(ctx, s.provider, req.RequestId, req.CompositeVolumeId, int64(req.CapacityBytes))
	if err != nil {
		return nil, rpcError(err)
	}
	return lease(r), nil
}

func (s *service) GetLease(ctx context.Context, key *pb.LeaseKey) (*pb.Lease, error) {
	if key.GetProvider() == "" || key.GetLeaseId() == "" || len(key.ProtoReflect().GetUnknown()) != 0 {
		return nil, invalid
	}
	if key.Provider != s.provider {
		return nil, denied
	}
	r, err := s.db.Lookup(ctx, key.Provider, key.LeaseId)
	// A closed dynamic lease returns its durable identity with ErrNotFound.
	// Authorize that identity before exposing authoritative closed semantics.
	if r.CompositeID != "" && !permitted(ctx, r.CompositeID) {
		return nil, denied
	}
	if err != nil {
		return nil, rpcError(err)
	}
	if !permitted(ctx, r.CompositeID) {
		return nil, denied
	}
	return lease(r), nil
}

func rpcError(err error) error {
	switch {
	case errors.Is(err, devicelease.ErrStale):
		return status.Error(codes.Aborted, "stale identity or epoch")
	case errors.Is(err, devicelease.ErrHeld):
		return status.Error(codes.FailedPrecondition, "ownership retained")
	case errors.Is(err, devicelease.ErrNotFound):
		return status.Error(codes.NotFound, "lease not found")
	case errors.Is(err, devicelease.ErrConflict):
		return status.Error(codes.AlreadyExists, "conflicting reservation")
	case errors.Is(err, devicelease.ErrExhausted):
		return status.Error(codes.ResourceExhausted, "insufficient reservable capacity")
	default:
		return status.Error(codes.Unavailable, "reservation state uncertain; retain and retry")
	}
}

func lease(r devicelease.Reservation) *pb.Lease {
	l := &pb.Lease{Allocation: &pb.Allocation{Key: &pb.LeaseKey{Provider: r.PoolUUID, LeaseId: r.LeaseID},
		CompositeVolumeId: r.CompositeID, ResourceId: r.ResourceID(),
		StableDeviceId: r.StableDeviceID, DeviceGeneration: r.Generation},
		PoolId: devicelease.Group, CapacityBytes: uint64(r.CapacityBytes), State: pb.LeaseState_LEASE_STATE_RESERVED}
	if r.Handle != "" {
		l.PoolId = devicelease.OrdinaryPool
	}
	if c := r.Claim; c != nil {
		l.OwnerEpoch = c.Epoch
		if c.Phase != "" {
			l.NodeId, l.RenewByUnixSeconds = c.Node, c.Deadline
			l.State = pb.LeaseState_LEASE_STATE_HELD
			if c.Phase == "acquired" {
				l.State = pb.LeaseState_LEASE_STATE_ACQUIRED
				l.ObservedDevicePath = "/dev/" + l.Allocation.ResourceId
				l.ObservedMajor, l.ObservedMinor = c.Major, c.Minor
			}
		}
	}
	return l
}
