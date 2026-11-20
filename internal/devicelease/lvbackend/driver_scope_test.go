// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"testing"

	"go.uber.org/mock/gomock"

	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

func TestCanaryPoolRejectsManagedAddonOwnership(t *testing.T) {
	const driver = "localdisk.dmstack.infinitydb.io"
	tag := devicelease.OrdinaryVGTag(driver)
	for _, tags := range []string{tag, "local-csi", tag + ",local-csi", "another-owner"} {
		t.Run(tags, func(t *testing.T) {
			cfg := devicelease.DynamicConfig{VolumeGroup: "canary-vg",
				PoolUUID: "aaaaaa-bbbb-cccc-dddd-eeee-ffff-gggggg", CacheDriver: driver}
			manager := lvmMgr.NewMockManager(gomock.NewController(t))
			manager.EXPECT().GetVolumeGroup(gomock.Any(), cfg.VolumeGroup).Return(
				&lvmMgr.VolumeGroup{Name: cfg.VolumeGroup, UUID: cfg.PoolUUID, Attributes: "wz--n-", Tags: tags}, nil)
			err := (Backend{Manager: manager, DriverName: driver}).CheckOrdinaryPool(t.Context(), cfg)
			if (err == nil) != (tags == tag) {
				t.Fatal("canary would overlap managed ownership", tags, err)
			}
		})
	}
	manager := lvmMgr.NewMockManager(gomock.NewController(t))
	if err := (Backend{Manager: manager, DriverName: devicelease.OfficialCacheDriver}).CheckOrdinaryPool(t.Context(),
		devicelease.DynamicConfig{VolumeGroup: "canary-vg", PoolUUID: "aaaaaa-bbbb-cccc-dddd-eeee-ffff-gggggg", CacheDriver: driver}); err == nil {
		t.Fatal("configured alias differed from actual serving driver")
	}
}
