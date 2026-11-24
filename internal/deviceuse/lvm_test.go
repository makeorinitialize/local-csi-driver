// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"testing"

	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"

	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

func TestNativeLVMObservesExistingInactiveVolumeWithoutProvisioning(t *testing.T) {
	const vgUUID = "aaaaaa-bbbb-cccc-dddd-eeee-ffff-gggggg"
	const lvUUID = "hhhhhh-iiii-jjjj-kkkk-llll-mmmm-nnnnnn"
	for _, kind := range []string{"existing-inactive", "missing", "foreign-vg", "quarantined"} {
		t.Run(kind, func(t *testing.T) {
			manager := lvmMgr.NewMockManager(gomock.NewController(t))
			vg := &lvmMgr.VolumeGroup{Name: "vg", UUID: vgUUID, Tags: "local-csi", Attributes: "wz--n-"}
			lv := &lvmMgr.LogicalVolume{Name: "lv", VGName: "vg", UUID: lvUUID, Size: 4096, Attributes: "-wi-------"}
			if kind == "foreign-vg" {
				vg.Tags = "foreign"
			}
			manager.EXPECT().GetVolumeGroup(gomock.Any(), "vg").Return(vg, nil)
			if kind != "foreign-vg" {
				if kind == "missing" {
					lv = nil
				}
				if kind == "quarantined" {
					lv.Tags = "wipe-pending"
				}
				manager.EXPECT().GetLogicalVolume(gomock.Any(), "vg", "lv").Return(lv, nil)
			}
			observe := NativeLVM(manager, OfficialCacheDriver, "")
			pv := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: OfficialCacheDriver, VolumeHandle: "vg#lv"}}}}
			resource, err := observe(t.Context(), pv, &corev1.Node{})
			if kind == "existing-inactive" {
				if err != nil || resource.Key != vgUUID+"/"+lvUUID || resource.Target != "vg#lv" {
					t.Fatal(resource, err)
				}
			} else if err == nil {
				t.Fatal("unverified native identity accepted", kind)
			}
		})
	}
}
