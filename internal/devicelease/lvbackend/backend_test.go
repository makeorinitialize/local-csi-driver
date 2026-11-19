// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"context"
	"os"
	"strings"
	"testing"

	"go.uber.org/mock/gomock"

	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

func TestRawCacheReserveWithManager(t *testing.T) {
	m := lvmMgr.NewMockManager(gomock.NewController(t))
	vg := &lvmMgr.VolumeGroup{Name: devicelease.Group, UUID: "vg-id", PVCount: 1,
		ExtentSize: 4096, ExtentCount: 2, ExtentFreeCount: 2, Size: 8192, Attributes: "wz--n-", Free: 8192, Tags: devicelease.PoolTag}
	var inventory []lvmMgr.LogicalVolume
	m.EXPECT().GetVolumeGroup(gomock.Any(), devicelease.Group).Return(vg, nil).AnyTimes()
	m.EXPECT().ListPhysicalVolumes(gomock.Any(), &lvmMgr.ListPVOptions{Select: "vg_name=" + devicelease.Group}).
		DoAndReturn(func(context.Context, *lvmMgr.ListPVOptions) ([]lvmMgr.PhysicalVolume, error) {
			return []lvmMgr.PhysicalVolume{{UUID: "pv-1", VGName: vg.Name, Attributes: "a--", ExtentCount: 2,
				ExtentAllocCount: 2 - vg.ExtentFreeCount}}, nil
		}).AnyTimes()
	m.EXPECT().ListLogicalVolumes(gomock.Any(), &lvmMgr.ListLVOptions{Names: []string{devicelease.Group}}).
		DoAndReturn(func(context.Context, *lvmMgr.ListLVOptions) ([]lvmMgr.LogicalVolume, error) { return inventory, nil }).AnyTimes()
	m.EXPECT().CreateLogicalVolume(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, o lvmMgr.CreateLVOptions) (int64, error) {
		if o.VGName != devicelease.Group || o.Extents != "2" || o.Size != "" || o.Activate != lvmMgr.No ||
			o.SetActivationSkip != lvmMgr.Yes || o.SetAutoActivation != lvmMgr.No || len(o.Tags) != 3 {
			t.Fatalf("unsafe lvcreate: %+v", o)
		}
		vg.LVCount, vg.Free, vg.ExtentFreeCount = 1, 0, 0
		inventory = []lvmMgr.LogicalVolume{{VGName: vg.Name, Name: o.Name, UUID: "stable", Tags: strings.Join(o.Tags, ","),
			Size: 8192, Attributes: "-wi------k", SkipActivation: true}}
		return 8192, nil
	})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p, err := devicelease.Open(dir, Backend{Manager: m})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err := p.Reserve(t.Context(), "request", "vol", 4097)
	if err != nil || r.StableDeviceID != "stable" || r.CapacityBytes != 8192 {
		t.Fatal(r, err)
	}
	live, err := p.Lookup(t.Context(), r.PoolUUID, r.LeaseID)
	if live != r || err != nil {
		t.Fatal(live, err)
	}
}

func TestRawCacheNeverEnumeratesOrdinaryVG(t *testing.T) {
	m := lvmMgr.NewMockManager(gomock.NewController(t))
	m.EXPECT().GetVolumeGroup(gomock.Any(), devicelease.Group).Return(&lvmMgr.VolumeGroup{
		Name: devicelease.Group, UUID: "vg-id", PVCount: 1, ExtentSize: 4096, Tags: "local-csi",
	}, nil)
	if _, err := (Backend{Manager: m}).Inspect(t.Context()); err == nil {
		t.Fatal("accepted ordinary VG")
	}
}

func TestRawCacheReservableExtents(t *testing.T) {
	m := lvmMgr.NewMockManager(gomock.NewController(t))
	b := Backend{Manager: m}
	vg := &lvmMgr.VolumeGroup{Name: devicelease.Group, PVCount: 2, LVCount: 1,
		Attributes: "wz--n-", Size: 16384, ExtentSize: 4096, ExtentCount: 4, ExtentFreeCount: 3, Free: 12288}
	pvs := []lvmMgr.PhysicalVolume{
		{UUID: "a", VGName: vg.Name, ExtentCount: 2, ExtentAllocCount: 1, Attributes: "a--"},
		{UUID: "b", VGName: vg.Name, ExtentCount: 2, Attributes: "---"},
	}
	m.EXPECT().ListPhysicalVolumes(gomock.Any(), &lvmMgr.ListPVOptions{Select: "vg_name=" + devicelease.Group}).Return(pvs, nil).Times(3)
	if free, err := b.reservable(t.Context(), vg); err != nil || free != 4096 {
		t.Fatal(free, err)
	}
	vg.MaxLogicalVolumes = 1
	if free, err := b.reservable(t.Context(), vg); err != nil || free != 0 {
		t.Fatal(free, err)
	}
	pvs[1].UUID = "a"
	if _, err := b.reservable(t.Context(), vg); err == nil {
		t.Fatal("duplicate PV accepted as confirmed capacity")
	}
}
