// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
	mount "k8s.io/mount-utils"

	"local-csi-driver/internal/devicelease"
)

// DiscoverOrdinary obtains the UUID from the actual LV, never from Kubernetes
// annotations or a caller-supplied stable ID. InspectOrdinary then verifies the
// full VG/LV/sysfs identity before the provider attempts an exclusive open.
func (b Backend) DiscoverOrdinary(ctx context.Context, e devicelease.Enrollment) (devicelease.Enrollment, error) {
	vg, lv, _ := strings.Cut(e.VolumeHandle, "#")
	if !devicelease.OrdinaryTarget(vg, lv) {
		return e, devicelease.ErrUncertain
	}
	l, err := b.Manager.GetLogicalVolume(ctx, vg, lv)
	if err != nil || l == nil || !lvmID.MatchString(l.UUID) {
		return e, errors.Join(devicelease.ErrUncertain, err)
	}
	e.LVUUID = l.UUID
	_, err = b.InspectOrdinary(ctx, e)
	return e, err
}

func (b Backend) InspectOrdinary(ctx context.Context, e devicelease.Enrollment) (devicelease.Volume, error) {
	vg, lv, _ := strings.Cut(e.VolumeHandle, "#")
	if !devicelease.OrdinaryTarget(vg, lv) || !lvmID.MatchString(e.VGUUID) || !lvmID.MatchString(e.LVUUID) {
		return devicelease.Volume{}, devicelease.ErrUncertain
	}
	g, err := b.Manager.GetVolumeGroup(ctx, vg)
	if err != nil {
		return devicelease.Volume{}, err
	}
	if g == nil || g.Name != vg || g.UUID != e.VGUUID || len(g.Attributes) != 6 || g.Attributes[0] != 'w' || g.Attributes[2:4] != "--" || g.Attributes[5] != '-' ||
		bool(g.Partial) || bool(g.Shared) || bool(g.Clustered) || bool(g.Exported) || g.MissingPVCount != 0 ||
		!slices.Contains(strings.Split(g.Tags, ","), "local-csi") {
		return devicelease.Volume{}, devicelease.ErrUncertain
	}
	l, err := b.Manager.GetLogicalVolume(ctx, vg, lv)
	if err != nil {
		return devicelease.Volume{}, err
	}
	if l == nil || l.VGName != vg || l.Name != lv || l.UUID != e.LVUUID || int64(l.Size) != e.Bytes || l.Tags != "" ||
		len(l.Attributes) != 10 || l.Attributes[1] != 'w' || l.Attributes[4] != 'a' || (l.Attributes[5] != '-' && l.Attributes[5] != 'o') ||
		l.Attributes[8] != '-' || !bool(l.ActiveLocally) || bool(l.ActiveRemotely) {
		return devicelease.Volume{}, devicelease.ErrUncertain
	}
	if l.Attributes[0] != '-' && !(l.Attributes[0] == 'r' && slices.Contains(strings.Split(l.Layout, ","), "raid0")) {
		return devicelease.Volume{}, devicelease.ErrUncertain
	}
	major, e1 := strconv.ParseUint(l.KernelMajor, 10, 32)
	minor, e2 := strconv.ParseUint(l.KernelMinor, 10, 32)
	if e1 != nil || e2 != nil || major == 0 {
		return devicelease.Volume{}, devicelease.ErrUncertain
	}
	v := devicelease.Volume{Name: lv, UUID: l.UUID, Bytes: e.Bytes, Active: true, Open: l.Attributes[5] == 'o', Major: uint32(major), Minor: uint32(minor)}
	dm, err := deviceUUID(v.Major, v.Minor)
	if err != nil || dm != "LVM-"+strings.ReplaceAll(e.VGUUID+e.LVUUID, "-", "") {
		return devicelease.Volume{}, errors.Join(devicelease.ErrUncertain, err)
	}
	return v, nil
}
func deviceUUID(major, minor uint32) (string, error) {
	const root = "/sys/dev/block"
	var fs unix.Statfs_t
	if err := unix.Statfs(root, &fs); err != nil {
		return "", err
	}
	if fs.Type != unix.SYSFS_MAGIC {
		return "", devicelease.ErrUncertain
	}
	b, err := os.ReadFile(fmt.Sprintf("%s/%d:%d/dm/uuid", root, major, minor))
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", devicelease.ErrUncertain
	}
	return id, nil
}
func (b Backend) BlockTarget(path string) (*devicelease.BlockTarget, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, nil
		}
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK {
		return nil, nil
	}
	t := &devicelease.BlockTarget{Major: unix.Major(st.Rdev), Minor: unix.Minor(st.Rdev)}
	id, err := deviceUUID(t.Major, t.Minor)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	t.DMUUID = id
	return t, nil
}
func (b Backend) WithExclusive(e devicelease.Enrollment, v devicelease.Volume, fn func() error) error {
	if err := noHolders(v); err != nil {
		return err
	}
	vg, lv, _ := strings.Cut(e.VolumeHandle, "#")
	if !devicelease.OrdinaryTarget(vg, lv) {
		return devicelease.ErrUncertain
	}
	fd, err := unix.Open("/dev/"+vg+"/"+lv, unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.EBUSY) {
		return devicelease.ErrHeld
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := VerifyDescriptor(fd, e.VGUUID, e.LVUUID, v); err != nil {
		return err
	}
	// A kubelet bind of a raw block inode need not be an open LV or a
	// kernel holder. Inspect its actual rdev in the host mount namespace.
	mounts, err := mount.ParseMountInfo("/proc/1/mountinfo")
	if err != nil || len(mounts) == 0 {
		return errors.Join(devicelease.ErrUncertain, err)
	}
	if err := unpublished(v, mounts, unix.Stat); err != nil {
		return err
	}
	return fn()
}

func unpublished(v devicelease.Volume, mounts []mount.MountInfo, stat func(string, *unix.Stat_t) error) error {
	for _, m := range mounts {
		if int(v.Major) == m.Major && int(v.Minor) == m.Minor {
			return devicelease.ErrHeld
		}
		if !strings.HasPrefix(m.MountPoint, "/var/lib/kubelet/") || !strings.Contains(m.MountPoint, "/volumeDevices/") {
			continue
		}
		if strings.Contains(m.MountPoint, `\`) {
			return devicelease.ErrUncertain
		}
		var st unix.Stat_t
		if err := stat("/proc/1/root"+m.MountPoint, &st); err != nil {
			return err
		}
		if int(unix.Major(st.Dev)) != m.Major || int(unix.Minor(st.Dev)) != m.Minor {
			return devicelease.ErrUncertain
		}
		if st.Mode&unix.S_IFMT == unix.S_IFBLK && unix.Major(st.Rdev) == v.Major && unix.Minor(st.Rdev) == v.Minor {
			return devicelease.ErrHeld
		}
	}
	return nil
}
