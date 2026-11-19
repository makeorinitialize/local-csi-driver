// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"errors"
	"os"
	"testing"

	"go.uber.org/mock/gomock"

	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

func TestActivationIsSelectedAndValidationRejectsNonBlockFD(t *testing.T) {
	m := lvmMgr.NewMockManager(gomock.NewController(t))
	b := Backend{Manager: m}
	r := devicelease.Reservation{PoolUUID: "aaaaaa-aaaa-aaaa-aaaa-aaaa-aaaa-aaaaaa",
		StableDeviceID: "bbbbbb-bbbb-bbbb-bbbb-bbbb-bbbb-bbbbbb", LeaseID: "0123456789abcdef0123456789abcdef", Generation: 11}
	m.EXPECT().UpdateLogicalVolume(gomock.Any(), lvmMgr.UpdateLVOptions{
		Name: "acs-dmstack-raw-v1/raw-" + r.LeaseID, Activate: lvmMgr.Yes, IgnoreActivationSkip: true,
		Select: "vg_uuid=" + r.PoolUUID + " && lv_uuid=" + r.StableDeviceID + " && lv_tags=acs.dmstack.gen.11 && lv_tags=acs.dmstack.lease." + r.LeaseID,
	}).Return(nil)
	if err := b.Activate(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	r.LeaseID = "../other"
	if err := b.Activate(t.Context(), r); !errors.Is(err, devicelease.ErrUncertain) {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "opened")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := VerifyDescriptor(int(f.Fd()), r.PoolUUID, r.StableDeviceID, devicelease.Volume{}); !errors.Is(err, devicelease.ErrUncertain) {
		t.Fatal(err)
	}
}
