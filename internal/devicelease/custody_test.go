// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type localDisk struct {
	*disk
	starts, stops, wipes int
	uncertain            bool
}

func (d *localDisk) Activate(context.Context, Reservation) error {
	d.starts++
	v := &d.Volumes[0]
	v.Inactive, v.Active, v.Major, v.Minor = false, true, 253, 9
	return nil
}
func (d *localDisk) Prepare(_ context.Context, _ Reservation, _ Volume, first bool) error {
	if first {
		d.wipes++
	}
	return nil
}
func (d *localDisk) Deactivate(context.Context, Reservation, Volume) error {
	d.stops++
	d.Volumes[0].Active, d.Volumes[0].Inactive = false, true
	return nil
}
func (d *localDisk) ConfirmInactive(context.Context, Reservation) error {
	if d.uncertain {
		return ErrUncertain
	}
	return nil
}

type proof struct {
	owner Owner
	open  bool
}

func (p proof) Enrolled(o Owner) error {
	if o != p.owner {
		return ErrHeld
	}
	return nil
}
func (p proof) Opened(o Owner, _ Volume) error {
	if !p.open {
		return ErrUncertain
	}
	return p.Enrolled(o)
}
func (p proof) Closed(o Owner, id string) error {
	if id != "receipt" || p.open {
		return ErrHeld
	}
	return p.Enrolled(o)
}

func TestLocalOwnerLifecycle(t *testing.T) {
	p, d := setup(t)
	ctx := t.Context()
	dm := &localDisk{disk: d}
	p.backend = dm
	r, err := p.Reserve(ctx, "reserve", "vol", 123)
	if err != nil {
		t.Fatal(err)
	}
	o := Owner{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID, StableDeviceID: r.StableDeviceID,
		ResourceID: Group + "/" + r.name(), NodeID: "local", Generation: r.Generation, Epoch: 1}
	proof := proof{owner: o}
	readJournal := func() []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(p.directory, "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := readJournal()
	if _, err := p.Acquire(ctx, nil, "acq", o, 0); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, proof, "reserve", o, 0); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	wrong := o
	wrong.Generation++
	if _, err := p.Acquire(ctx, proof, "wrong", wrong, 0); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if _, err := p.Acquire(ctx, proof, "acq", o, 42); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readJournal()) {
		t.Fatal("unadmitted calls changed the journal")
	}
	if dm.starts != 0 {
		t.Fatal("unverified activation")
	}
	a, err := p.Acquire(ctx, proof, "acq", o, 0)
	if err != nil || a.Claim.Phase != "acquired" {
		t.Fatal(a, err)
	}
	restart := Provider{directory: p.directory, backend: dm}
	again, err := restart.Acquire(ctx, proof, "acq", o, 0)
	if err != nil || *again.Claim != *a.Claim || dm.starts != 1 || dm.wipes != 1 {
		t.Fatal(again, err)
	}
	if _, err := restart.Renew(ctx, proof, "acq", o); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	d.Volumes[0].Open = true
	if err := restart.ValidateOpen(ctx, proof, o, 4096, 253, 9); !errors.Is(err, ErrUncertain) {
		t.Fatal("accepted echoed values", err)
	}
	proof.open = true
	if err := restart.ValidateOpen(ctx, proof, o, 4096, 253, 9); err != nil {
		t.Fatal(err)
	}
	if err := restart.ValidateOpen(ctx, proof, o, 4096, 253, 8); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if err := withLedger(p.directory, func(s *ledger, save func() error) error { s.Records["vol"].Claim.Deadline = 1; return save() }); err != nil {
		t.Fatal(err)
	}
	if _, err := restart.Renew(ctx, proof, "renew", o); !errors.Is(err, ErrHeld) {
		t.Fatal(err)
	}
	live, err := restart.Lookup(ctx, o.PoolUUID, o.LeaseID)
	if err != nil || live.Claim.Phase != "held" || dm.stops != 0 {
		t.Fatal(live, err)
	}
	proof.open = false
	if err := restart.ReleaseNode(ctx, proof, "release", o, "receipt"); !errors.Is(err, ErrHeld) || dm.stops != 0 {
		t.Fatal("open mapping deactivated", err)
	}
	d.Volumes[0].Open = false
	if err := restart.ReleaseNode(ctx, proof, "bad-receipt", o, "assertion"); !errors.Is(err, ErrHeld) || dm.stops != 0 {
		t.Fatal(err)
	}
	dm.uncertain = true
	if err := restart.ReleaseNode(ctx, proof, "release", o, "receipt"); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if err := restart.ReleaseNode(ctx, proof, "release", o, "changed"); !errors.Is(err, ErrConflict) {
		t.Fatal("admitted uncertain intent lost its binding", err)
	}
	if _, err := restart.Lookup(ctx, o.PoolUUID, o.LeaseID); !errors.Is(err, ErrUncertain) {
		t.Fatal("ambiguous cleanup was committed", err)
	}
	dm.uncertain = false
	if err := restart.ReleaseNode(ctx, proof, "release", o, "receipt"); err != nil || dm.stops != 1 {
		t.Fatal(err)
	}
	next := o
	next.Epoch = 2
	proof.owner = next
	if _, err := restart.Acquire(ctx, proof, "next", next, 1); err != nil || dm.starts != 2 || dm.wipes != 1 {
		t.Fatal(err)
	}
	if err := restart.ReleaseNode(ctx, proof, "stale", o, "receipt"); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if err := restart.ReleaseNode(ctx, proof, "release", o, "receipt"); err != nil || dm.stops != 1 {
		t.Fatal("historical replay disturbed successor", err)
	}
}
