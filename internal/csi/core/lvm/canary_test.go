// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvm

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/mock/gomock"

	"local-csi-driver/internal/deviceuse"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
	"local-csi-driver/internal/pkg/probe"
	"local-csi-driver/internal/pkg/telemetry"
)

func TestCanaryRegistrationAndExclusiveVG(t *testing.T) {
	const name, group = "localdisk.compositedisk.infinitydb.io", "compositedisk-canary"
	if err := ConfigureCanary(name, group); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ConfigureCanary(OfficialDriverName, "") })
	manager := lvmMgr.NewMockManager(gomock.NewController(t))
	l, err := New("pod", "node", "namespace", true, probe.NewFake(nil, nil), manager, telemetry.NewNoopTracerProvider())
	if err != nil {
		t.Fatal(err)
	}
	if l.GetDriverName() != name || l.GetCSIDriver().Name != name || DefaultVolumeGroupTag == "local-csi" ||
		DefaultVolumeGroupTag != deviceuse.VGTag(name) {
		t.Fatal("canary would register or scan as the managed addon")
	}
	lv := "pvc-12345678-1234-1234-1234-123456789abc"
	manager.EXPECT().GetLogicalVolume(gomock.Any(), group, lv).Return(&lvmMgr.LogicalVolume{Name: lv, VGName: group, Size: 4096}, nil)
	manager.EXPECT().IsLogicalVolumeCorrupted(gomock.Any(), group, lv).Return(false, nil)
	v, err := l.Create(t.Context(), &csi.CreateVolumeRequest{Name: lv, CapacityRange: &csi.CapacityRange{RequiredBytes: 4096}})
	if err != nil || v.VolumeId != group+"#"+lv || v.AccessibleTopology[0].Segments["topology."+name+"/node"] != "node" ||
		v.VolumeContext[name+"/capacity"] != "4096" {
		t.Fatal("actual CreateVolume did not use independent CSI identity", v, err)
	}
	for _, id := range []string{"containerstorage#" + lv, "other#" + lv, "mapper#compositedisk--canary-" + lv, group + "#../" + lv} {
		if _, _, err := l.BeginNative(t.Context(), "DeleteVolume", id, ""); err == nil {
			t.Fatal("accepted foreign/alias native operation", id)
		}
		if _, err := l.GetNodeDevicePath(id); err == nil {
			t.Fatal("returned foreign/alias device", id)
		}
	}
	if _, err := l.EnsurePhysicalVolumes(t.Context(), group); err == nil {
		t.Fatal("canary discovered/reinitialized physical disks")
	}
	if _, err := l.EnsureVolumeGroup(t.Context(), group, []string{"/not-a-device"}); err == nil {
		t.Fatal("canary initialized a pool")
	}
	if err := l.Cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n, err := l.AvailableCapacity(t.Context(), "containerstorage"); err != nil || n != 0 {
		t.Fatal("inspected managed addon's capacity", n, err)
	}
	manager.EXPECT().ListVolumeGroups(gomock.Any(), &lvmMgr.ListVGOptions{Select: "vg_tags=" + DefaultVolumeGroupTag}).
		Return([]lvmMgr.VolumeGroup{{Name: group}, {Name: "containerstorage"}}, nil)
	groups, err := ManagedVolumeGroups(t.Context(), manager)
	if err != nil || len(groups) != 1 || groups[0] != group {
		t.Fatal("GC escaped the private VG", groups, err)
	}
}
