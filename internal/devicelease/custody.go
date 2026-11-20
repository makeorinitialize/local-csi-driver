// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"errors"
	"time"
)

var (
	ErrStale = errors.New("stale local allocation or epoch")
	ErrHeld  = errors.New("local ownership retained")
)

type Owner struct {
	PoolUUID, CompositeID, LeaseID, ResourceID, StableDeviceID, NodeID string
	Generation, Epoch                                                  uint64
}

type Claim struct {
	Node, Phase  string
	Epoch        uint64
	Deadline     int64
	Major, Minor uint32
	Request      string
}

type mutation struct {
	Method   string
	Owner    Owner
	Previous uint64
	Receipt  string
	Done     bool
}

type LocalMedia interface {
	Activate(context.Context, Reservation) error
	Prepare(context.Context, Reservation, Volume, bool) error
	Deactivate(context.Context, Reservation, Volume) error
	ConfirmInactive(context.Context, Reservation) error
}

// Authority is independent of the requesting facade. Absence, even on an
// otherwise idle node, cannot attest either exclusive access or teardown.
type Authority interface {
	Enrolled(Owner) error
	Opened(context.Context, Owner, Volume) error
	Closed(Owner, string) error
}

func (s *ledger) validateClaims() error {
	for id, m := range s.Mutations {
		if !validID(id) || s.Requests[id].CompositeID != "" || !m.Owner.valid() ||
			(m.Method != "acquire" && m.Method != "renew" && m.Method != "release") {
			return ErrUncertain
		}
	}
	for _, r := range s.Records {
		c := r.Claim
		if c == nil {
			continue
		}
		if c.Epoch == 0 || r.StableDeviceID == "" {
			return ErrUncertain
		}
		m, ok := s.Mutations[c.Request]
		if !ok || !m.Owner.matches(r) || m.Owner.Epoch != c.Epoch {
			return ErrUncertain
		}
		switch c.Phase {
		case "":
			if c.Node != "" || c.Deadline != 0 || m.Method != "release" || !m.Done {
				return ErrUncertain
			}
		case "preparing", "acquired", "held", "closing":
			if c.Node == "" || c.Node != m.Owner.NodeID {
				return ErrUncertain
			}
			if c.Phase == "preparing" && (m.Method != "acquire" || m.Done) ||
				c.Phase == "closing" && (m.Method != "release" || m.Done) ||
				(c.Phase == "acquired" || c.Phase == "held") && (c.Deadline <= 0 || !m.Done || m.Method == "release") {
				return ErrUncertain
			}
		default:
			return ErrUncertain
		}
	}
	return nil
}

func (o Owner) valid() bool {
	return o.PoolUUID != "" && o.CompositeID != "" && o.LeaseID != "" && o.ResourceID != "" &&
		o.StableDeviceID != "" && o.NodeID != "" && o.Generation != 0 && o.Epoch != 0
}
func (o Owner) matches(r Reservation) bool {
	return o.PoolUUID == r.PoolUUID && o.CompositeID == r.CompositeID && o.LeaseID == r.LeaseID &&
		o.ResourceID == r.ResourceID() && o.StableDeviceID == r.StableDeviceID && o.Generation == r.Generation
}

func (p Provider) inventory(ctx context.Context, s *ledger, r Reservation) (Volume, error) {
	return p.inventoryFor(ctx, s, r, false)
}

func (p Provider) inventoryFor(ctx context.Context, s *ledger, r Reservation, receiptOnly bool) (Volume, error) {
	if p.isOrdinary() {
		if err := p.checkCanary(s); err != nil {
			return Volume{}, err
		}
		if err := p.sourceCurrent(ctx, s, receiptOnly); err != nil {
			return Volume{}, err
		}
		return observe(ctx, p.observer, *s.Canary)
	}
	pool, err := p.backend.Inspect(ctx)
	if err != nil {
		return Volume{}, errors.Join(ErrUncertain, err)
	}
	volumes, err := s.reconcile(pool)
	if err != nil {
		return Volume{}, err
	}
	return volumes[r.CompositeID], nil
}

func (p Provider) Acquire(ctx context.Context, a Authority, id string, o Owner, previous uint64) (Reservation, error) {
	return p.change(ctx, a, id, mutation{Method: "acquire", Owner: o, Previous: previous})
}
func (p Provider) Renew(ctx context.Context, a Authority, id string, o Owner) (Reservation, error) {
	return p.change(ctx, a, id, mutation{Method: "renew", Owner: o})
}
func (p Provider) ReleaseNode(ctx context.Context, a Authority, id string, o Owner, receipt string) error {
	if receipt == "" {
		return ErrHeld
	}
	_, err := p.change(ctx, a, id, mutation{Method: "release", Owner: o, Receipt: receipt})
	return err
}

func (p Provider) change(ctx context.Context, a Authority, id string, m mutation) (Reservation, error) {
	media, ok := p.backend.(LocalMedia)
	if (!ok && !p.isOrdinary()) || a == nil || !validID(id) || !m.Owner.valid() {
		return Reservation{}, ErrUncertain
	}
	o := m.Owner
	var result Reservation
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.isOrdinary() {
			if err := p.checkCanary(s); err != nil {
				return err
			}
			if o.NodeID != s.Canary.NodeID {
				return ErrStale
			}
		}
		if o.PoolUUID != s.PoolUUID {
			return ErrStale
		}
		if s.Requests[id].CompositeID != "" {
			return ErrConflict
		}
		old, known := s.Mutations[id]
		done := old.Done
		old.Done = false
		if known && old != m {
			return ErrConflict
		}
		if s.Dynamic != nil && s.Dynamic.HostBinding != nil && s.Dynamic.Host != nil {
			if err := p.bindHostAuthority(s, a, o, false); err != nil {
				return err
			}
		}
		// Bind a fresh ID in the same snapshot as its first admitted
		// transition, before any backend effect. Rejected keys cost no space.
		record := func() {
			if s.Canary == nil {
				s.Version = 2
			}
			if s.Mutations == nil {
				s.Mutations = map[string]mutation{}
			}
			s.Mutations[id] = m
		}
		if done && m.Method == "release" {
			return nil
		} // historical ack only
		if s.Retired {
			return ErrStale
		}
		r := s.Records[o.CompositeID]
		if !o.matches(r) {
			return ErrStale
		}
		v, err := p.inventoryFor(ctx, s, r, m.Method == "release")
		if err != nil {
			return err
		}
		c := r.Claim
		if c == nil {
			c = &Claim{}
			r.Claim = c
		}
		if c.Phase == "acquired" && time.Now().Unix() >= c.Deadline {
			c.Phase = "held"
			if err := save(); err != nil {
				return err
			}
		}
		same := c.Node == o.NodeID && c.Epoch == o.Epoch
		if done {
			if !same {
				return ErrStale
			}
			if c.Phase != "acquired" {
				return ErrHeld
			}
			if !live(c, v) {
				return ErrUncertain
			}
			if err := authorizeLive(a, o, c, save); err != nil {
				return err
			}
			result = r
			return nil
		}
		switch m.Method {
		case "acquire":
			if c.Phase != "" {
				return ErrHeld
			}
			if c.Epoch != m.Previous || o.Epoch <= c.Epoch {
				return ErrStale
			}
			if s.Canary != nil {
				if s.Dynamic != nil {
					if err := p.bindHostAuthority(s, a, o, true); err != nil {
						return err
					}
					// Cancellation must distinguish "never acquired" from a
					// dispatched authority/probe with an unknown outcome.
					*c = Claim{Phase: "preparing", Node: o.NodeID, Epoch: o.Epoch, Request: id}
					s.Records[o.CompositeID] = r
					record()
					if err := save(); err != nil {
						return err
					}
				}
				if err := a.Enrolled(o); err != nil {
					return err
				}
				if err := withUnused(p.observer, *s.Canary, v, func() error { return nil }); err != nil {
					return err
				}
				*c = Claim{Phase: "acquired", Node: o.NodeID, Epoch: o.Epoch, Request: id, Major: v.Major, Minor: v.Minor, Deadline: time.Now().Add(time.Minute).Unix()}
				s.Records[o.CompositeID] = r
				break
			}
			if !v.Inactive {
				return ErrUncertain
			}
			if err := a.Enrolled(o); err != nil {
				return err
			}
			first := c.Epoch == 0
			*c = Claim{Phase: "preparing", Node: o.NodeID, Epoch: o.Epoch, Request: id}
			s.Records[o.CompositeID] = r
			record()
			if err := save(); err != nil {
				return err
			}
			// A crash or error here is retained, not a license to activate or
			// zero a second time. No device hint exists until durable commit.
			if err := media.Activate(ctx, r); err != nil {
				return errors.Join(ErrUncertain, err)
			}
			v, err = p.inventory(ctx, s, r)
			if err != nil {
				return err
			}
			if !v.Active || v.Open {
				return ErrHeld
			}
			if err := media.Prepare(ctx, r, v, first); err != nil {
				return errors.Join(ErrUncertain, err)
			}
			c.Major, c.Minor = v.Major, v.Minor
			v, err = p.inventory(ctx, s, r)
			if err != nil {
				return err
			}
			if !live(c, v) || v.Open {
				return ErrUncertain
			}
			if err := a.Enrolled(o); err != nil {
				return err
			}
			c.Phase = "acquired"
			c.Deadline = time.Now().Add(time.Minute).Unix()
		case "renew":
			if !same {
				return ErrStale
			}
			if c.Phase != "acquired" {
				return ErrHeld
			}
			if !live(c, v) {
				return ErrUncertain
			}
			if err := authorizeLive(a, o, c, save); err != nil {
				return err
			}
			c.Deadline = time.Now().Add(time.Minute).Unix()
		case "release":
			if !same {
				return ErrStale
			}
			if c.Phase != "acquired" && c.Phase != "held" && !(c.Phase == "closing" && c.Request == id) {
				return ErrHeld
			}
			if err := a.Closed(o, m.Receipt); err != nil {
				return err
			}
			if s.Canary != nil {
				v, err = p.inventoryFor(ctx, s, r, true)
				if err != nil {
					return err
				}
				if !live(c, v) {
					return ErrUncertain
				}
				if err := withUnused(p.observer, *s.Canary, v, func() error { return nil }); err != nil {
					return err
				}
				s.Retired = true
				*c = Claim{Epoch: o.Epoch, Request: id}
				break
			}
			// A dispatched command may have outlived its parent. Do not queue
			// another deactivation against a name a successor could later use.
			if c.Phase == "closing" && !v.Inactive {
				return ErrUncertain
			}
			if c.Phase != "closing" {
				if !live(c, v) || v.Open {
					return ErrHeld
				}
				c.Phase, c.Request = "closing", id
				record()
				if err := save(); err != nil {
					return err
				}
			}
			if !v.Inactive {
				if !live(c, v) || v.Open {
					return ErrHeld
				}
				if err := media.Deactivate(ctx, r, v); err != nil {
					return errors.Join(ErrUncertain, err)
				}
				v, err = p.inventory(ctx, s, r)
				if err != nil {
					return err
				}
				if !v.Inactive {
					return ErrUncertain
				}
			}
			if err := media.ConfirmInactive(ctx, r); err != nil {
				return errors.Join(ErrUncertain, err)
			}
			*c = Claim{Epoch: o.Epoch, Request: id}
		}
		m.Done = true
		record()
		if err := save(); err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		return Reservation{}, err
	}
	return result, nil
}

func live(c *Claim, v Volume) bool { return v.Active && c.Major == v.Major && c.Minor == v.Minor }

func authorizeLive(a Authority, o Owner, c *Claim, save func() error) error {
	if err := a.Enrolled(o); err != nil {
		return err
	}
	return beforeDeadline(c, save)
}

func beforeDeadline(c *Claim, save func() error) error {
	if time.Now().Unix() < c.Deadline {
		return nil
	}
	c.Phase = "held"
	if err := save(); err != nil {
		return err
	}
	return ErrHeld
}

func snapshot(r Reservation, v Volume, save func() error) error {
	c := r.Claim
	if c == nil || c.Phase == "" {
		return nil
	}
	if c.Phase == "preparing" || c.Phase == "closing" {
		return ErrUncertain
	}
	if c.Phase == "acquired" && time.Now().Unix() >= c.Deadline {
		c.Phase = "held"
		if err := save(); err != nil {
			return err
		}
	}
	if c.Phase == "acquired" && !live(c, v) {
		return ErrUncertain
	}
	return nil
}

func (p Provider) ValidateOpen(ctx context.Context, a Authority, o Owner, size uint64, major, minor uint32) error {
	if a == nil {
		return ErrUncertain
	}
	return withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.bindHostAuthority(s, a, o, false); err != nil {
			return err
		}
		r := s.Records[o.CompositeID]
		if !o.valid() || !o.matches(r) || r.Claim == nil || r.Claim.Epoch != o.Epoch || r.Claim.Node != o.NodeID {
			return ErrStale
		}
		c := r.Claim
		if c.Phase != "acquired" {
			return ErrHeld
		}
		if time.Now().Unix() >= c.Deadline {
			c.Phase = "held"
			if err := save(); err != nil {
				return err
			}
			return ErrHeld
		}
		v, err := p.inventory(ctx, s, r)
		if err != nil {
			return err
		}
		if !live(c, v) || !v.Open || uint64(v.Bytes) != size || v.Major != major || v.Minor != minor {
			return ErrUncertain
		}
		if err := a.Opened(ctx, o, v); err != nil {
			return err
		}
		return beforeDeadline(c, save)
	})
}
