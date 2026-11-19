// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Only the LVM command boundary is stubbed. Instances share no in-process
// lock and must serialize through the same on-disk journal.
type disk struct {
	Pool
	mu        sync.Mutex
	calls     int
	createErr error
}

func (d *disk) Inspect(context.Context) (Pool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Pool, nil
}
func (d *disk) CreateInactive(_ context.Context, name string, tags []string, extents int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	d.Volumes = append(d.Volumes, Volume{Name: name, Tags: tags, UUID: fmt.Sprint("lv-", d.calls), Bytes: extents * d.ExtentBytes, Inactive: true})
	d.FreeBytes -= extents * d.ExtentBytes
	return d.createErr
}

func setup(t *testing.T) (Provider, *disk) {
	t.Helper()
	d := &disk{Pool: Pool{UUID: "vg-uuid", ExtentBytes: 4096, FreeBytes: 8192}}
	p := Provider{directory: t.TempDir(), backend: d}
	if err := p.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	return p, d
}

func TestConcurrentReservations(t *testing.T) {
	p, d := setup(t)
	var wg sync.WaitGroup
	var results [8]Reservation
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			local := Provider{directory: p.directory, backend: d}
			var err error
			results[i], err = local.Reserve(t.Context(), fmt.Sprint(i), "first", 4095)
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	for _, r := range results {
		if r != results[0] || r.CapacityBytes != 4096 || r.StableDeviceID != "lv-1" {
			t.Fatalf("unexpected result %+v", r)
		}
	}
	if d.calls != 1 {
		t.Fatal("duplicate creation")
	}
	for _, tc := range []struct {
		request, composite string
		bytes              int64
	}{
		{"0", "next", 4095}, {"0", "first", 4096}, {"new", "first", 4096}, {"new", "next", 4096},
	} {
		if _, err := p.Reserve(t.Context(), tc.request, tc.composite, tc.bytes); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	r, err := p.Reserve(t.Context(), "second", "next", 1)
	if err != nil || r.Generation <= results[0].Generation || r.LeaseID == results[0].LeaseID {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := p.Reserve(t.Context(), "third", "three", 1); !errors.Is(err, ErrExhausted) {
		t.Fatal(err)
	}
}

func TestLostReplyAndUncertainIdentity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drift func(*disk)
	}{
		{"missing", func(d *disk) { d.Volumes = nil }},
		{"replaced", func(d *disk) { d.Volumes[0].UUID = "replacement" }},
		{"generation", func(d *disk) { d.Volumes[0].Tags[1] = "acs.dmstack.gen.2" }},
		{"active", func(d *disk) { d.Volumes[0].Inactive = false }},
		{"pool", func(d *disk) { d.UUID = "other-vg" }},
		{"foreign", func(d *disk) { d.Volumes = append(d.Volumes, Volume{Name: "pvc-foreign"}) }},
		{"alias", func(d *disk) { d.Volumes = append(d.Volumes, d.Volumes[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, d := setup(t)
			d.createErr = context.Canceled
			if r, err := p.Reserve(t.Context(), "id", "vol", 123); !errors.Is(err, ErrUncertain) || r != (Reservation{}) {
				t.Fatal(r, err)
			}
			restarted := Provider{directory: p.directory, backend: d}
			r, err := restarted.Reserve(t.Context(), "id", "vol", 123)
			if err != nil || r.Generation != 1 || r.StableDeviceID != "lv-1" {
				t.Fatal(r, err)
			}
			if live, err := restarted.Lookup(t.Context(), r.PoolUUID, r.LeaseID); live != r || err != nil {
				t.Fatal(live, err)
			}
			tc.drift(d)
			if live, err := restarted.Lookup(t.Context(), r.PoolUUID, r.LeaseID); live != (Reservation{}) || !errors.Is(err, ErrUncertain) {
				t.Fatal(live, err)
			}
			if r, err := restarted.Reserve(t.Context(), "id", "vol", 123); !errors.Is(err, ErrUncertain) || r != (Reservation{}) {
				t.Fatal(r, err)
			}
			if d.calls != 1 {
				t.Fatal("replayed create after identity drift")
			}
		})
	}
}

func TestMissingIntentOrLedgerNeverReallocates(t *testing.T) {
	p, d := setup(t)
	d.createErr = context.Canceled
	if _, err := p.Reserve(t.Context(), "id", "vol", 1); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	lvs := d.Volumes
	d.Volumes = nil
	if _, err := p.Reserve(t.Context(), "id", "vol", 1); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	d.Volumes = lvs
	path := filepath.Join(p.directory, "ledger.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reserve(t.Context(), "id", "vol", 1); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reserve(t.Context(), "id", "vol", 1); !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if err := p.Initialize(t.Context()); err == nil || d.calls != 1 {
		t.Fatal("adopted existing media")
	}
}
