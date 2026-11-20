// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import "context"

// Allocation is the immutable DeviceProvider allocation tuple, not a path or
// a request to delete media. No field is normalized on receipt.
type Allocation struct {
	PoolUUID, CompositeID, LeaseID, ResourceID, StableDeviceID string
	Generation                                                 uint64
}

func (a Allocation) valid() bool {
	return validID(a.PoolUUID) && validID(a.CompositeID) && validID(a.LeaseID) &&
		validID(a.ResourceID) && validID(a.StableDeviceID) && a.Generation != 0
}

func (a Allocation) matches(r Reservation) bool {
	return a.PoolUUID == r.PoolUUID && a.CompositeID == r.CompositeID && a.LeaseID == r.LeaseID &&
		a.ResourceID == r.ResourceID() && a.StableDeviceID == r.StableDeviceID && a.Generation == r.Generation
}

type allocationClosure struct {
	RequestID  string
	Allocation Allocation
	Epoch      uint64
}

// closeEligible uses positive journal evidence only. Absence of a node receipt,
// an expired deadline, missing Kubernetes objects or missing media is NOT proof
// that an acquisition never occurred or that a former owner released it.
func closeEligible(s *ledger, r Reservation, epoch uint64) error {
	if s.Dynamic == nil || s.Dynamic.Admission || s.Canary == nil || s.Dynamic.Source == nil {
		return ErrUncertain
	}
	if r.Claim == nil {
		if epoch != 0 {
			return ErrStale
		}
		if len(s.Mutations) != 0 {
			return ErrUncertain
		}
		return nil
	}
	c := r.Claim
	if epoch != c.Epoch {
		return ErrStale
	}
	if epoch == 0 || !s.Retired || c.Phase != "" || c.Node != "" || c.Deadline != 0 {
		return ErrHeld
	}
	m, ok := s.Mutations[c.Request]
	if !ok || m.Method != "release" || !m.Done || m.Receipt == "" || !m.Owner.matches(r) ||
		m.Owner.Epoch != epoch || m.Owner.NodeID != s.Canary.NodeID {
		return ErrUncertain
	}
	for _, pending := range s.Mutations {
		if !pending.Done {
			return ErrUncertain
		}
	}
	return nil
}

func (s *ledger) validateClosure() error {
	if s.Dynamic == nil || s.Dynamic.Closure == nil {
		return nil
	}
	c := s.Dynamic.Closure
	r, ok := s.Records[c.Allocation.CompositeID]
	if !ok || !s.Retired || !validID(c.RequestID) || !c.Allocation.valid() || !c.Allocation.matches(r) ||
		s.Requests[c.RequestID].CompositeID != "" {
		return ErrUncertain
	}
	if _, conflict := s.Mutations[c.RequestID]; conflict {
		return ErrUncertain
	}
	if closeEligible(s, r, c.Epoch) != nil {
		return ErrUncertain
	}
	return nil
}

// ReleaseAllocation irreversibly closes use authorization for this one-shot
// dynamic domain. It never observes, opens, unmounts, deactivates or deletes an
// LV. The ordinary CSI/PVC path owns physical reclamation after this commit.
func (p Provider) ReleaseAllocation(ctx context.Context, request string, a Allocation, epoch uint64) error {
	if p.dynamic == nil || !validID(request) || !a.valid() {
		return ErrUncertain
	}
	want := allocationClosure{RequestID: request, Allocation: a, Epoch: epoch}
	return withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.checkDynamic(s); err != nil {
			return err
		}
		// Exact committed replay is history-only, even after CR/PVC/LV removal.
		if closed := s.Dynamic.Closure; closed != nil {
			if *closed != want {
				return ErrConflict
			}
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.Dynamic.Admission || s.Canary == nil {
			return ErrUncertain
		}
		r, ok := s.Records[a.CompositeID]
		if !ok || !a.matches(r) {
			return ErrStale
		}
		if _, conflict := s.Requests[request]; conflict {
			return ErrConflict
		}
		if _, conflict := s.Mutations[request]; conflict {
			return ErrConflict
		}
		if err := closeEligible(s, r, epoch); err != nil {
			return err
		}
		s.Dynamic.Closure, s.Retired = &want, true
		return save()
	})
}
