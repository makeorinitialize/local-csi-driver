// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Package lvbackend binds the reservation store to the real ACS LVM manager.
package lvbackend

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

type Backend struct{ Manager lvmMgr.Manager }

func (b Backend) Inspect(ctx context.Context) (devicelease.Pool, error) {
	vg, err := b.Manager.GetVolumeGroup(ctx, devicelease.Group)
	if err != nil {
		return devicelease.Pool{}, err
	}
	if vg == nil || vg.Name != devicelease.Group || vg.UUID == "" || vg.Partial || vg.Shared || vg.Clustered ||
		vg.Exported || vg.MissingPVCount != 0 || vg.PVCount < 1 || vg.Free < 0 || vg.ExtentSize <= 0 ||
		len(vg.Attributes) != 6 || vg.Attributes[0] != 'w' || vg.Attributes[2:4] != "--" || vg.Attributes[5] != '-' ||
		vg.Size <= 0 || vg.Size < vg.Free || vg.ExtentCount <= 0 || int64(vg.Size/vg.ExtentSize) != int64(vg.ExtentCount) ||
		int64(vg.Free/vg.ExtentSize) != int64(vg.ExtentFreeCount) || vg.Tags != devicelease.PoolTag {
		return devicelease.Pool{}, fmt.Errorf("expected a complete, local, exclusively provider-tagged VG")
	}
	lvs, err := b.Manager.ListLogicalVolumes(ctx, &lvmMgr.ListLVOptions{Names: []string{devicelease.Group}})
	if err != nil {
		return devicelease.Pool{}, err
	}
	if len(lvs) != int(vg.LVCount) {
		return devicelease.Pool{}, fmt.Errorf("inconsistent LV inventory")
	}
	free, err := b.reservable(ctx, vg)
	if err != nil {
		return devicelease.Pool{}, err
	}
	p := devicelease.Pool{UUID: vg.UUID, FreeBytes: free, ExtentBytes: int64(vg.ExtentSize)}
	for _, lv := range lvs {
		if lv.VGName != vg.Name {
			return devicelease.Pool{}, fmt.Errorf("unexpected LV group")
		}
		inactive := len(lv.Attributes) == 10 && lv.Attributes[0] == '-' && lv.Attributes[4:6] == "--" &&
			bool(lv.SkipActivation) && !bool(lv.ActiveLocally) && !bool(lv.ActiveRemotely)
		v := devicelease.Volume{Name: lv.Name, UUID: lv.UUID, Bytes: int64(lv.Size),
			Tags: strings.Split(lv.Tags, ","), Inactive: inactive}
		if len(lv.Attributes) == 10 && lv.Attributes[0] == '-' && lv.Attributes[4] == 'a' && lv.Attributes[8] == '-' &&
			(lv.Attributes[5] == '-' || lv.Attributes[5] == 'o') && lv.SkipActivation && lv.ActiveLocally && !lv.ActiveRemotely {
			major, e1 := strconv.ParseUint(lv.KernelMajor, 10, 32)
			minor, e2 := strconv.ParseUint(lv.KernelMinor, 10, 32)
			if e1 != nil || e2 != nil || major == 0 {
				return devicelease.Pool{}, fmt.Errorf("unknown active LV device number")
			}
			v.Active, v.Open, v.Major, v.Minor = true, lv.Attributes[5] == 'o', uint32(major), uint32(minor)
		}
		p.Volumes = append(p.Volumes, v)
	}
	return p, nil
}

func (b Backend) reservable(ctx context.Context, vg *lvmMgr.VolumeGroup) (int64, error) {
	if vg.Attributes[4] != 'n' || vg.MaxLogicalVolumes < 0 || vg.Size%vg.ExtentSize != 0 || vg.Free%vg.ExtentSize != 0 {
		return 0, fmt.Errorf("unsupported VG allocation policy or invalid extent inventory")
	}
	pvs, err := b.Manager.ListPhysicalVolumes(ctx, &lvmMgr.ListPVOptions{Select: "vg_name=" + devicelease.Group})
	if err != nil {
		return 0, err
	}
	if len(pvs) != int(vg.PVCount) {
		return 0, fmt.Errorf("incomplete PV inventory")
	}
	total, free, usable := int64(0), int64(0), int64(0)
	seen := map[string]bool{}
	for _, pv := range pvs {
		if pv.VGName != vg.Name || pv.UUID == "" || seen[pv.UUID] || bool(pv.Duplicate) || bool(pv.Exported) || bool(pv.Missing) ||
			pv.ExtentCount <= 0 || pv.ExtentAllocCount < 0 || pv.ExtentAllocCount > pv.ExtentCount ||
			int64(pv.ExtentCount) > int64(vg.ExtentCount)-total ||
			(pv.Attributes != "a--" && pv.Attributes != "---") {
			return 0, fmt.Errorf("inconsistent PV identity or extent inventory")
		}
		seen[pv.UUID] = true
		total += int64(pv.ExtentCount)
		available := int64(pv.ExtentCount - pv.ExtentAllocCount)
		free += available
		if pv.Attributes == "a--" {
			usable += available
		}
	}
	if total != int64(vg.ExtentCount) || free != int64(vg.ExtentFreeCount) {
		return 0, fmt.Errorf("VG/PV extent mismatch")
	}
	if vg.MaxLogicalVolumes != 0 && vg.LVCount >= vg.MaxLogicalVolumes {
		return 0, nil
	}
	return usable * int64(vg.ExtentSize), nil
}

func (b Backend) CreateInactive(ctx context.Context, name string, tags []string, extents int64) error {
	_, err := b.Manager.CreateLogicalVolume(ctx, lvmMgr.CreateLVOptions{
		VGName: devicelease.Group, Name: name, Extents: strconv.FormatInt(extents, 10), Tags: tags, Type: "linear",
		Activate: lvmMgr.No, SetActivationSkip: lvmMgr.Yes, SetAutoActivation: lvmMgr.No,
		Zero: lvmMgr.No, WipeSignatures: lvmMgr.No,
	})
	return err
}
