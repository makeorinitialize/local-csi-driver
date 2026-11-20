// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvm

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"local-csi-driver/internal/devicelease"
)

const OfficialDriverName = devicelease.OfficialCacheDriver

var canaryVolumeGroup string

// ConfigureCanary is a process bootstrap, called before any manager/server
// starts. Separate instances are separate processes, never runtime aliases of
// the managed addon's name or LVM ownership domain.
func ConfigureCanary(name, group string) error {
	if name == OfficialDriverName {
		if group != "" {
			return fmt.Errorf("a private VG requires a separately registered CSI name")
		}
		DriverName, DefaultVolumeGroup, DefaultVolumeGroupTag = OfficialDriverName, "containerstorage", "local-csi"
		canaryVolumeGroup = ""
		csiDriver.Spec.VolumeLifecycleModes = []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecyclePersistent, storagev1.VolumeLifecycleEphemeral}
	} else {
		if len(name) == 0 || len(name) > 63 || len(validation.IsDNS1123Subdomain(name)) != 0 ||
			name == devicelease.OfficialOriginDriver || group == "containerstorage" || group == "mapper" ||
			!devicelease.OrdinaryTarget(group, "volume") {
			return fmt.Errorf("canary requires an exact separate CSI name and private nondefault VG")
		}
		DriverName, DefaultVolumeGroup, DefaultVolumeGroupTag = name, group, devicelease.OrdinaryVGTag(name)
		canaryVolumeGroup = group
		csiDriver.Spec.VolumeLifecycleModes = []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecyclePersistent}
	}
	csiDriver.Name = DriverName
	TopologyKey = "topology." + DriverName + "/node"
	CapacityParam, LimitParam, ExpandedCapacityParam = DriverName+"/capacity", DriverName+"/limit", DriverName+"/expanded-capacity"
	return nil
}

func checkCanaryTarget(id string) error {
	if canaryVolumeGroup == "" {
		return nil
	}
	vg, lv, ok := strings.Cut(id, "#")
	if !ok || vg != canaryVolumeGroup || !devicelease.OrdinaryTarget(vg, lv) {
		return status.Error(codes.FailedPrecondition, "outside the private canonical canary VG")
	}
	return nil
}

func canaryGroupAllowed(group string) bool {
	return canaryVolumeGroup == "" || group == canaryVolumeGroup
}
