// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

var (
	ErrConflict  = errors.New("reservation conflicts with recorded intent")
	ErrUncertain = errors.New("raw cache state uncertain; retain reservation")
	ErrExhausted = errors.New("insufficient free extents")
	ErrNotFound  = errors.New("reservation not found")
)

// Backend cannot mount, activate, resize, release or enumerate PVCs.
type Backend interface {
	Inspect(context.Context) (Pool, error)
	CreateInactive(ctx context.Context, name string, tags []string, extents int64) error
}

type Pool struct {
	UUID                   string
	FreeBytes, ExtentBytes int64
	Volumes                []Volume
}

type Volume struct {
	Name, UUID   string
	Bytes        int64
	Tags         []string
	Inactive     bool
	Active, Open bool
	Major, Minor uint32
}

// Reservation has no device path. A reservation is never an IO grant.
type Reservation struct {
	Handle                                         string `json:",omitempty"`
	PoolUUID, CompositeID, LeaseID, StableDeviceID string
	Generation                                     uint64
	RequestedBytes, CapacityBytes                  int64
	Claim                                          *Claim `json:",omitempty"`
}

func (r Reservation) name() string { return "raw-" + r.LeaseID }
func (r Reservation) tags() []string {
	return []string{ownerTag, "acs.dmstack.gen." + strconv.FormatUint(r.Generation, 10), "acs.dmstack.lease." + r.LeaseID}
}

type ledger struct {
	Canary   *Enrollment `json:",omitempty"`
	Retired  bool        `json:",omitempty"`
	Version  int
	PoolUUID string
	// No record or request key is ever removed in v1.
	Records   map[string]Reservation // composite -> incarnation
	Requests  map[string]reserveIntent
	Mutations map[string]mutation `json:",omitempty"`
}

type reserveIntent struct {
	CompositeID string
	Bytes       int64
}

type Provider struct {
	ordinary  *Enrollment
	observer  OrdinaryObserver
	directory string // fixed, private node-persistent directory
	backend   Backend
}

func validID(s string) bool { return s != "" && utf8.ValidString(s) }

// Capacity reports only space available for another linear reservation in
// this provider's exact pool. Unknown inventory is not zero capacity.
func (p Provider) Capacity(ctx context.Context, poolUUID string) (int64, error) {
	if p.ordinary != nil {
		_, n, err := p.ordinaryQuery(ctx, poolUUID, "", "", "", 0)
		return n, err
	}
	var free int64
	err := withLedger(p.directory, func(s *ledger, _ func() error) error {
		if poolUUID != s.PoolUUID {
			return ErrUncertain
		}
		pool, err := p.backend.Inspect(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUncertain, err)
		}
		if _, err := s.reconcile(pool); err != nil {
			return err
		}
		for _, r := range s.Records {
			if r.StableDeviceID == "" {
				return ErrUncertain
			}
		}
		free = pool.FreeBytes
		return nil
	})
	return free, err
}

// Lookup checks the physical inventory against the journal, including the LV
// UUID and generation. It cannot commit a pending creation or validate IO.
func (p Provider) Lookup(ctx context.Context, poolUUID, leaseID string) (Reservation, error) {
	if !validID(poolUUID) || !validID(leaseID) {
		return Reservation{}, errors.New("pool UUID and lease ID required")
	}
	if p.ordinary != nil {
		r, _, err := p.ordinaryQuery(ctx, poolUUID, "", "", leaseID, 0)
		return r, err
	}
	var result Reservation
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		pool, err := p.backend.Inspect(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUncertain, err)
		}
		found, err := s.reconcile(pool)
		if err != nil {
			return err
		}
		if poolUUID != pool.UUID {
			return fmt.Errorf("%w: wrong local provider", ErrUncertain)
		}
		for _, r := range s.Records {
			if r.LeaseID == leaseID {
				if r.StableDeviceID == "" {
					return ErrUncertain
				}
				result = r
				return snapshot(r, found[r.CompositeID], save)
			}
		}
		return ErrNotFound
	})
	if err != nil {
		return Reservation{}, err
	}
	return result, nil
}

// Reserve serializes durable admission and the one create attempt across
// processes. An ambiguous intent with no LV is *not* retried destructively.
func (p Provider) Reserve(ctx context.Context, request, composite string, bytes int64) (Reservation, error) {
	return p.reserve(ctx, "", request, composite, bytes)
}

// ReserveInPool compares the transport's provider identity under the same
// lock as request admission. A reconfigured endpoint cannot relabel a lease.
func (p Provider) ReserveInPool(ctx context.Context, poolUUID, request, composite string, bytes int64) (Reservation, error) {
	if poolUUID == "" {
		return Reservation{}, ErrUncertain
	}
	return p.reserve(ctx, poolUUID, request, composite, bytes)
}

func (p Provider) reserve(ctx context.Context, poolUUID, request, composite string, bytes int64) (Reservation, error) {
	if !validID(request) || !validID(composite) || bytes <= 0 {
		return Reservation{}, errors.New("request ID, composite ID and positive byte count required")
	}
	if p.ordinary != nil {
		r, _, err := p.ordinaryQuery(ctx, poolUUID, request, composite, "", bytes)
		return r, err
	}
	var result Reservation
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if poolUUID != "" && poolUUID != s.PoolUUID {
			return ErrUncertain
		}
		input := reserveIntent{CompositeID: composite, Bytes: bytes}
		if _, ok := s.Mutations[request]; ok {
			return ErrConflict
		}
		if old, ok := s.Requests[request]; ok {
			if old != input {
				return ErrConflict
			}
		} else {
			s.Requests[request] = input
			if err := save(); err != nil {
				return err
			}
		}
		r, exists := s.Records[composite]
		if exists && r.RequestedBytes != bytes {
			return ErrConflict
		}
		pool, err := p.backend.Inspect(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUncertain, err)
		}
		found, err := s.reconcile(pool)
		if err != nil {
			return err
		}
		if !exists {
			if bytes > pool.FreeBytes {
				return ErrExhausted
			}
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			r = Reservation{PoolUUID: pool.UUID, CompositeID: composite, LeaseID: hex.EncodeToString(id[:]),
				Generation: uint64(len(s.Records)) + 1, RequestedBytes: bytes}
			s.Records[composite] = r
			if err := save(); err != nil {
				return err
			}
			if err := p.backend.CreateInactive(ctx, r.name(), r.tags(), 1+(bytes-1)/pool.ExtentBytes); err != nil {
				return fmt.Errorf("%w: lvcreate: %w", ErrUncertain, err)
			}
			pool, err = p.backend.Inspect(ctx)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrUncertain, err)
			}
			found, err = s.reconcile(pool)
			if err != nil {
				return err
			}
		}
		lv, ok := found[composite]
		if !ok {
			return fmt.Errorf("%w: dispatched creation not visible", ErrUncertain)
		}
		if r.StableDeviceID == "" {
			r.StableDeviceID, r.CapacityBytes = lv.UUID, lv.Bytes
			s.Records[composite] = r
			if err := save(); err != nil {
				return err
			}
		}
		result = r
		return snapshot(r, lv, save)
	})
	if err != nil {
		return Reservation{}, err
	}
	return result, nil
}

// Never adopt unknown volumes, assume a missing committed LV is free, or
// identify an LV by its path. Reject aliases anywhere in this dedicated pool.
func (s *ledger) reconcile(pool Pool) (map[string]Volume, error) {
	if pool.UUID == "" || pool.FreeBytes < 0 || pool.ExtentBytes < 1 || pool.FreeBytes%pool.ExtentBytes != 0 || s.PoolUUID != pool.UUID {
		return nil, fmt.Errorf("%w: pool identity or capacity", ErrUncertain)
	}
	byName := make(map[string]Reservation, len(s.Records))
	for _, r := range s.Records {
		byName[r.name()] = r
	}
	found := make(map[string]Volume, len(pool.Volumes))
	ids := make(map[string]bool)
	for _, lv := range pool.Volumes {
		r, ok := byName[lv.Name]
		activeAllowed := r.Claim != nil && r.Claim.Phase != ""
		if !ok || (!lv.Inactive && !(activeAllowed && lv.Active)) || lv.UUID == "" || ids[lv.UUID] || lv.Bytes < r.RequestedBytes ||
			(r.StableDeviceID != "" && (r.StableDeviceID != lv.UUID || r.CapacityBytes != lv.Bytes)) ||
			!matchTags(lv.Tags, r.tags()) {
			return nil, fmt.Errorf("%w: unexpected LV or changed identity", ErrUncertain)
		}
		if _, dup := found[r.CompositeID]; dup {
			return nil, ErrUncertain
		}
		found[r.CompositeID], ids[lv.UUID] = lv, true
	}
	for composite := range s.Records {
		if _, ok := found[composite]; !ok {
			return nil, fmt.Errorf("%w: dispatched LV missing", ErrUncertain)
		}
	}
	return found, nil
}

func matchTags(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	tags := map[string]bool{}
	for _, t := range got {
		tags[t] = true
	}
	for _, t := range want {
		if !tags[t] {
			return false
		}
	}
	return true
}

func (s *ledger) validate() error {
	if s.Canary != nil {
		e := s.Canary
		if s.Version != 3 || !e.valid() || e.VGUUID != s.PoolUUID || len(s.Records) > 1 {
			return ErrUncertain
		}
		for _, r := range s.Records {
			if r.Handle != e.VolumeHandle || r.CompositeID != e.CompositeID || r.Generation != e.Generation || r.StableDeviceID != e.LVUUID || r.CapacityBytes != e.Bytes ||
				s.Retired != (r.Claim != nil && r.Claim.Phase == "") {
				return ErrUncertain
			}
		}
	} else if (s.Version != 1 && s.Version != 2) || (s.Version == 2) != (len(s.Mutations) > 0) || s.Retired {
		return ErrUncertain
	}
	if s.Records == nil || s.Requests == nil || s.PoolUUID == "" {
		return ErrUncertain
	}
	seen := map[string]bool{}
	gens := map[uint64]bool{}
	intents := map[reserveIntent]bool{}
	for k, input := range s.Requests {
		if !validID(k) || !validID(input.CompositeID) || input.Bytes <= 0 {
			return ErrUncertain
		}
		intents[input] = true
	}
	for c, r := range s.Records {
		id, err := hex.DecodeString(r.LeaseID)
		if !validID(c) || c != r.CompositeID || r.PoolUUID != s.PoolUUID || !intents[reserveIntent{c, r.RequestedBytes}] ||
			err != nil || len(id) != 16 || hex.EncodeToString(id) != r.LeaseID || seen[r.LeaseID] ||
			r.Generation == 0 || s.Canary == nil && (r.Generation > uint64(len(s.Records)) || r.Handle != "") || gens[r.Generation] ||
			r.RequestedBytes <= 0 || (r.StableDeviceID == "") != (r.CapacityBytes == 0) ||
			(r.CapacityBytes != 0 && r.CapacityBytes < r.RequestedBytes) {
			return ErrUncertain
		}
		seen[r.LeaseID], gens[r.Generation] = true, true
	}
	return s.validateClaims()
}
