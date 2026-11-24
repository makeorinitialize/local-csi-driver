// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"

	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

const OfficialCacheDriver = "localdisk.csi.acstor.io"

// Never adopt the retired private allocator's media into ordinary CSI/GC.
const RetiredRawVG = "acs-compositedisk-raw-v1"

var nativeName = regexp.MustCompile(`^[A-Za-z0-9+_.][A-Za-z0-9+_.-]*$`)
var nativeUUID = regexp.MustCompile(`^[A-Za-z0-9]{6}(?:-[A-Za-z0-9]{4}){5}-[A-Za-z0-9]{6}$`)

func ValidTarget(vg, lv string) bool {
	return vg != "." && vg != ".." && lv != "." && lv != ".." &&
		len(vg) <= 127 && len(lv) <= 127 && nativeName.MatchString(vg) && nativeName.MatchString(lv) &&
		vg != RetiredRawVG && !(vg == "mapper" && strings.HasPrefix(lv, strings.ReplaceAll(RetiredRawVG, "-", "--")+"-"))
}

func VGTag(driver string) string {
	if driver == OfficialCacheDriver {
		return "local-csi"
	}
	return "compositedisk.csi." + driver
}

func NativeLVM(manager lvmMgr.Manager, driver, allowedGroup string) func(context.Context, *corev1.PersistentVolume, *corev1.Node) (Resource, error) {
	return func(ctx context.Context, pv *corev1.PersistentVolume, _ *corev1.Node) (Resource, error) {
		bad := status.Error(codes.FailedPrecondition, "existing native VG/LV identity is not stable")
		vgName, lvName, ok := strings.Cut(pv.Spec.CSI.VolumeHandle, "#")
		if !ok || vgName == "mapper" || !ValidTarget(vgName, lvName) || allowedGroup != "" && vgName != allowedGroup {
			return Resource{}, bad
		}
		vg, err := manager.GetVolumeGroup(ctx, vgName)
		if err != nil {
			return Resource{}, err
		}
		tag := VGTag(driver)
		if vg == nil || vg.Name != vgName || !nativeUUID.MatchString(vg.UUID) ||
			len(vg.Attributes) != 6 || vg.Attributes[0] != 'w' || vg.Attributes[2:4] != "--" || vg.Attributes[5] != '-' ||
			bool(vg.Partial) || bool(vg.Shared) || bool(vg.Clustered) || bool(vg.Exported) || vg.MissingPVCount != 0 ||
			!slices.Contains(strings.Split(vg.Tags, ","), tag) ||
			tag != "local-csi" && slices.Contains(strings.Split(vg.Tags, ","), "local-csi") {
			return Resource{}, bad
		}
		lv, err := manager.GetLogicalVolume(ctx, vgName, lvName)
		if err != nil {
			return Resource{}, err
		}
		if lv == nil || lv.VGName != vgName || lv.Name != lvName || !nativeUUID.MatchString(lv.UUID) ||
			lv.Size == 0 || lv.Tags != "" || bool(lv.ActiveRemotely) || len(lv.Attributes) != 10 ||
			lv.Attributes[1] != 'w' || lv.Attributes[8] != '-' ||
			lv.Attributes[0] != '-' && !(lv.Attributes[0] == 'r' && slices.Contains(strings.Split(lv.Layout, ","), "raid0")) {
			return Resource{}, bad
		}
		return Resource{Key: vg.UUID + "/" + lv.UUID, Target: vg.Name + "#" + lv.Name, Group: vg.Name}, nil
	}
}
