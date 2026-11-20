// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DynamicConfig is operator configuration, never a CSI parameter. The domain
// is armed before ANY CSI/GC runnable starts. It admits exactly one incarnation.
type DynamicConfig struct {
	NodeName, NodeUID, Namespace, NamespaceUID string
	CacheClass, CacheClassUID                  string
	VolumeGroup, PoolUUID                      string
	Bytes                                      int64
	OriginDriver                               string `json:",omitempty"`
	CacheDriver                                string `json:",omitempty"`
}

func (c DynamicConfig) valid() bool {
	return validDrivers(c.OriginDriver, c.CacheDriver) && c.NodeName != "" && c.NodeUID != "" && c.Namespace != "" && c.Namespace != "default" &&
		c.NamespaceUID != "" && c.CacheClass != "" && c.CacheClassUID != "" &&
		c.VolumeGroup != "mapper" && OrdinaryTarget(c.VolumeGroup, "volume") && c.PoolUUID != "" && c.Bytes > 0 && c.Bytes%512 == 0
}

// Candidate contains only independently verified API identity, not an LV UUID
// or a device path. Fingerprint binds the complete canonical backing journal.
type Candidate struct {
	CompositeID, CompositeUID, VolumeHandle, Fingerprint, RequestID string
	Snapshot                                                        string `json:",omitempty"`
}

type CandidateReader interface {
	Resolve(context.Context, string, int64) (Candidate, error)
}

type OrdinaryDiscoverer interface {
	OrdinaryObserver
	DiscoverOrdinary(context.Context, Enrollment) (Enrollment, error)
}

type dynamicState struct {
	HostBinding *HostBinding `json:",omitempty"`
	Host        *hostRecord  `json:",omitempty"`
	Config      DynamicConfig
	Closure     *allocationClosure `json:",omitempty"`
	// Denied is append-only: a native command may survive process death.
	// Pending provisions are cleared ONLY by that invocation after success.
	Denied  map[string]bool
	Pending map[string]string // random invocation -> handle
	Source  *Candidate
	// Admission persists BEFORE acquiring the FD. A crash or failed commit
	// cannot turn a partially attempted enrollment back into free media.
	Admission bool
}

type dynamicProvider struct {
	config DynamicConfig
	reader CandidateReader
}

// OpenDynamic never upgrades an offline journal. Initial creation is only
// safe at driver startup, before handlers are exposed, with the persistent
// parent mount pinned by PinEnrollment. Missing journals in existing
// directories fail closed. Do not remove this directory to "reset" a slot.
func OpenDynamic(path string, c DynamicConfig, reader CandidateReader, b OrdinaryDiscoverer) (*Provider, error) {
	if !c.valid() || reader == nil || b == nil {
		return nil, ErrUncertain
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			return nil, err
		}
		defer parent.Close()
		if err := publishEnrollment(parent, path, func(dir *os.File) error {
			return saveLedger(dir, &ledger{Version: 5, HoldPolicy: HoldPolicyExplicitRelease, PoolUUID: c.PoolUUID,
				Records: map[string]Reservation{}, Requests: map[string]reserveIntent{},
				Dynamic: &dynamicState{Config: c, Denied: map[string]bool{}, Pending: map[string]string{}}})
		}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !privateDirectory(info) {
		return nil, errors.Join(ErrUncertain, err)
	}
	p := &Provider{directory: path, observer: b, dynamic: &dynamicProvider{c, reader}}
	err = withLedger(path, func(s *ledger, _ func() error) error { return p.checkDynamic(s) })
	return p, err
}

func (p *Provider) isOrdinary() bool { return p.ordinary != nil || p.dynamic != nil }

func (p *Provider) checkDynamic(s *ledger) error {
	// Legacy stores remain intact and protected; startup never upgrades a
	// timed or unmarked authorization into durable ownership.
	if s.Version != 5 || s.HoldPolicy != HoldPolicyExplicitRelease ||
		s.Dynamic == nil || !s.Dynamic.Config.same(p.dynamic.config) {
		return ErrUncertain
	}
	return nil
}

func (p *Provider) sourceCurrent(ctx context.Context, s *ledger, receiptOnly bool) error {
	if p.dynamic == nil {
		return nil
	}
	if err := p.checkDynamic(s); err != nil {
		return err
	}
	if s.Dynamic.Admission || s.Dynamic.Source == nil {
		return ErrUncertain
	}
	if receiptOnly {
		// Positive, previously verified immutable evidence, not an inference
		// from API absence. Actual media/owner checks still follow this.
		return s.validateSourceSnapshot()
	}
	now, err := p.dynamic.reader.Resolve(ctx, s.Canary.CompositeID, s.Canary.Bytes)
	if err != nil || now != *s.Dynamic.Source {
		return errors.Join(ErrUncertain, err)
	}
	return nil
}

func (p *Provider) dynamicQuery(ctx context.Context, pool, request, composite, id string, n int64) (Reservation, int64, error) {
	var result Reservation
	var available int64
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.checkDynamic(s); err != nil {
			return err
		}
		if pool != s.PoolUUID {
			return ErrStale
		}
		if s.Dynamic.Closure != nil {
			if id != "" {
				r := s.Records[s.Canary.CompositeID]
				if r.LeaseID != id {
					return ErrUncertain
				}
				// Return identity only so the RPC can authorize the closed key.
				result = r
				return ErrNotFound
			}
			if request != "" {
				return ErrHeld
			}
			return nil
		}
		if s.Canary == nil {
			// There is no independently inspected candidate for GetCapacity.
			if request == "" {
				if id != "" {
					return ErrUncertain
				}
				return nil
			}
			if err := p.enroll(ctx, s, save, request, composite, n); err != nil {
				return err
			}
		}
		if id != "" && s.Records[s.Canary.CompositeID].LeaseID != id {
			return ErrUncertain
		}
		if err := p.sourceCurrent(ctx, s, true); err != nil {
			return err
		}
		if request != "" && request != s.Dynamic.Source.RequestID {
			return ErrConflict
		}
		q := *p
		q.ordinary = s.Canary
		var err error
		result, available, err = q.queryOrdinary(ctx, s, save, pool, request, composite, id, n)
		return err
	})
	return result, available, err
}

func (p *Provider) enroll(ctx context.Context, s *ledger, save func() error, request, composite string, n int64) error {
	d := s.Dynamic
	if n != d.Config.Bytes {
		return ErrExhausted
	}
	c, err := p.dynamic.reader.Resolve(ctx, composite, n)
	if err != nil {
		return errors.Join(ErrUncertain, err)
	}
	vg, _, _ := strings.Cut(c.VolumeHandle, "#")
	if c.RequestID == "" || c.RequestID != request {
		return ErrConflict
	}
	if c.CompositeID != composite || c.CompositeUID == "" || c.Fingerprint == "" || vg != d.Config.VolumeGroup ||
		d.Denied["pool"] || d.Denied[c.VolumeHandle] || d.Denied[mapperID(c.VolumeHandle)] {
		return ErrHeld
	}
	for _, handle := range d.Pending {
		if handle == c.VolumeHandle || handle == mapperID(c.VolumeHandle) {
			return ErrUncertain
		}
	}
	e := Enrollment{NodeID: d.Config.NodeName, CompositeID: composite, VolumeHandle: c.VolumeHandle,
		VGUUID: s.PoolUUID, Generation: 1, VolumeMode: "Block", Bytes: n}
	e, err = p.observer.(OrdinaryDiscoverer).DiscoverOrdinary(ctx, e)
	if err != nil || !e.valid() || e.VolumeHandle != c.VolumeHandle || e.NodeID != d.Config.NodeName ||
		e.CompositeID != composite || e.VGUUID != s.PoolUUID || e.Generation != 1 || e.Bytes != n || e.VolumeMode != "Block" {
		return errors.Join(ErrUncertain, err)
	}
	v, err := observe(ctx, p.observer, e)
	if err != nil {
		return err
	}
	if d.Denied["uuid:"+e.LVUUID] || d.Denied[deviceKey(v.Major, v.Minor)] ||
		d.Denied["dm:"+dmID(e)] {
		return ErrHeld
	}
	// This additional durable state is essential: a read-only API snapshot
	// cannot atomically commit with our local journal or with CR deletion.
	// From here forward every failure retains the ONE slot and physical hold.
	s.Canary, d.Source, d.Admission = &e, &c, true
	if err := save(); err != nil {
		return err
	}
	return withUnused(p.observer, e, v, func() error {
		now, err := p.dynamic.reader.Resolve(ctx, composite, n)
		if err != nil || now != c {
			return errors.Join(ErrUncertain, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		r := Reservation{Handle: e.VolumeHandle, PoolUUID: e.VGUUID, CompositeID: composite,
			HoldPolicy: HoldPolicyExplicitRelease,
			LeaseID:    hex.EncodeToString(key[:]), StableDeviceID: e.LVUUID, Generation: e.Generation,
			RequestedBytes: n, CapacityBytes: n}
		s.Records[composite], s.Requests[request] = r, reserveIntent{composite, n}
		d.Admission = false
		// Exclusive descriptor spans the durable reservation commit.
		return save()
	})
}

func mapperID(handle string) string {
	vg, lv, _ := strings.Cut(handle, "#")
	return "mapper#" + strings.ReplaceAll(vg, "-", "--") + "-" + strings.ReplaceAll(lv, "-", "--")
}
func dmID(e Enrollment) string             { return "LVM-" + strings.ReplaceAll(e.VGUUID+e.LVUUID, "-", "") }
func deviceKey(major, minor uint32) string { return fmt.Sprintf("dev:%d:%d", major, minor) }

func (p *Provider) inDomain(id string) bool {
	vg, lv, ok := strings.Cut(id, "#")
	return ok && (vg == p.dynamic.config.VolumeGroup ||
		vg == "mapper" && strings.HasPrefix(lv, strings.ReplaceAll(p.dynamic.config.VolumeGroup, "-", "--")+"-"))
}

// Native admission is irreversible, not an unlocked check. Even a command
// admitted before the PVC/journal exists excludes this handle forever.
func (p *Provider) dynamicPhysical(id, uuid string, target *BlockTarget) error {
	return withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.checkDynamic(s); err != nil {
			return err
		}
		d := s.Dynamic
		if d.Closure != nil {
			return nil // terminal history cannot admit another provider owner
		}
		keys := []string{}
		if id == "pool" || p.inDomain(id) {
			keys = append(keys, id)
		}
		if uuid != "" {
			keys = append(keys, "uuid:"+uuid)
		}
		if target != nil {
			if target.DMUUID != "" {
				keys = append(keys, "dm:"+target.DMUUID)
			} else {
				keys = append(keys, deviceKey(target.Major, target.Minor))
			}
		}
		if e := s.Canary; e != nil && d.Closure == nil {
			match := id == "pool" || id == e.VolumeHandle || id == mapperID(e.VolumeHandle) || uuid != "" && uuid == e.LVUUID
			if target != nil {
				if target.DMUUID != "" {
					match = match || target.DMUUID == dmID(*e)
				} else {
					v, err := observe(context.Background(), p.observer, *e)
					if err != nil {
						return err
					}
					match = match || v.Major == target.Major && v.Minor == target.Minor
				}
			}
			if match {
				return ErrHeld
			}
		}
		changed := false
		for _, k := range keys {
			if !d.Denied[k] {
				d.Denied[k], changed = true, true
			}
		}
		if changed {
			return save()
		}
		return nil
	})
}

// BeginProvision is the sole non-retiring native admission. Provisioning must
// finish before a Bound PVC can be imported. Failure/crash keeps this exact
// allocation uncertain; it does not freeze unrelated handles.
func (p *Provider) BeginProvision(id string) (func(bool) error, error) {
	noop := func(bool) error { return nil }
	if p == nil || p.dynamic == nil {
		return noop, p.BeforePhysical(id, "")
	}
	if !p.inDomain(id) {
		return noop, nil
	}
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(key[:])
	closed := false
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.checkDynamic(s); err != nil {
			return err
		}
		if s.Dynamic.Closure != nil {
			closed = true
			return nil
		}
		if s.Canary != nil && (id == s.Canary.VolumeHandle || id == mapperID(s.Canary.VolumeHandle)) {
			return ErrHeld
		}
		s.Dynamic.Pending[token] = id
		return save()
	})
	if err != nil {
		return nil, err
	}
	if closed {
		return noop, nil
	}
	return func(success bool) error {
		if !success {
			return nil
		}
		return withLedger(p.directory, func(s *ledger, save func() error) error {
			if err := p.checkDynamic(s); err != nil {
				return err
			}
			if s.Dynamic.Pending[token] != id {
				return ErrUncertain
			}
			delete(s.Dynamic.Pending, token)
			return save()
		})
	}, nil
}

// BeforeGroupRemoval scopes retirement to the configured VG, not every ACS VG.
func (p *Provider) BeforeGroupRemoval(vg string) error {
	if p != nil && p.dynamic != nil {
		if vg == p.dynamic.config.VolumeGroup {
			return p.dynamicPhysical("pool", "", nil)
		}
		return nil
	}
	return p.BeforePoolRemoval()
}

// Pool setup/recovery can touch other LVs' mapper nodes. An online domain must
// already have the pinned VG; a dispatched setup/recovery command permanently
// rules it out, including commands that complete after enrollment is requested.
func (p *Provider) BeforeGroupMutation(vg string) error {
	if p != nil && p.dynamic != nil {
		return p.BeforeGroupRemoval(vg)
	}
	return nil
}

func (p *Provider) BeforeStats(id, path string) error {
	if p != nil && p.dynamic != nil {
		return p.BeforeUnpublish(id, path)
	}
	return nil
}
