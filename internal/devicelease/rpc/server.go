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
	db       *devicelease.Provider
	provider string // immutable local VG UUID; never a routing alias
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

func blockedOperation(ctx context.Context, req any, g Grant, provider string) error {
	var a *pb.Allocation
	switch r := req.(type) {
	case interface{ GetOwner() *pb.Owner }:
		if g.Role != "node" || r.GetOwner().GetNodeId() != g.NodeID {
			return denied
		}
		a = r.GetOwner().GetAllocation()
	case *pb.ReleaseRequest:
		switch r.Mode {
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
		return blocked
	}
	if a.GetKey().GetProvider() != provider || !permitted(ctx, a.GetCompositeVolumeId()) {
		return denied
	}
	return blocked
}

func (s *service) pool(p *pb.Pool) error {
	if p.GetProvider() == "" || p.GetPoolId() == "" || len(p.ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	if p.Provider != s.provider || p.PoolId != devicelease.Group {
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
	return &pb.Lease{Allocation: &pb.Allocation{Key: &pb.LeaseKey{Provider: r.PoolUUID, LeaseId: r.LeaseID},
		CompositeVolumeId: r.CompositeID, ResourceId: devicelease.Group + "/raw-" + r.LeaseID,
		StableDeviceId: r.StableDeviceID, DeviceGeneration: r.Generation},
		PoolId: devicelease.Group, CapacityBytes: uint64(r.CapacityBytes), State: pb.LeaseState_LEASE_STATE_RESERVED}
}
