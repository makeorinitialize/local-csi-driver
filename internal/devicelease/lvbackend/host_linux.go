// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
	mount "k8s.io/mount-utils"

	"local-csi-driver/internal/devicelease"
)

func (b Backend) OpenHost(ctx context.Context, e devicelease.Enrollment, v devicelease.Volume) (*os.File, error) {
	// Keeping a block O_EXCL claim across handoff would conflict with the
	// legitimate DM target claiming this bdev. Probe unused/exclusive first,
	// then retain an ordinary FD under the provider's durable native gate.
	vg, lv, _ := strings.Cut(e.VolumeHandle, "#")
	if !devicelease.OrdinaryTarget(vg, lv) {
		return nil, devicelease.ErrUncertain
	}
	var retained *os.File
	err := b.WithExclusive(e, v, func() error {
		fd, err := unix.Open("/dev/"+vg+"/"+lv, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		retained = os.NewFile(uintptr(fd), "acs-retained-lv")
		if err := b.VerifyHost(retained, e, v); err != nil {
			return err
		}
		return ctx.Err()
	})
	if err != nil {
		if retained != nil {
			retained.Close()
		}
		return nil, err
	}
	// Only the probe was released; this independently verified ordinary OFD
	// was already open, and the durable exclusive-use gate never dropped.
	return retained, nil
}

func (b Backend) VerifyHost(f *os.File, e devicelease.Enrollment, v devicelease.Volume) error {
	if f == nil {
		return devicelease.ErrUncertain
	}
	return VerifyDescriptor(int(f.Fd()), e.VGUUID, e.LVUUID, v)
}

// ConfirmHostUnused runs only after all member-owned descriptors are closed
// and future borrows are durably prohibited. The real LV open count includes
// borrowed descriptors, queued SCM_RIGHTS, mounts and other kernel users.
// Neither PID disappearance nor a client's assertion is closure evidence.
func (b Backend) ConfirmHostUnused(ctx context.Context, e devicelease.Enrollment, commit func() error) error {
	v, err := b.InspectOrdinary(ctx, e)
	if err != nil {
		return err
	}
	if v.Open {
		return devicelease.ErrHeld
	}
	return b.WithExclusive(e, v, func() error {
		if err := noNamespaceMounts(ctx, v); err != nil {
			return err
		}
		return commit()
	})
}

func noNamespaceMounts(ctx context.Context, v devicelease.Volume) error {
	var fs unix.Statfs_t
	if err := unix.Statfs("/proc", &fs); err != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return errors.Join(devicelease.ErrUncertain, err)
	}
	processes, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	type namespace struct{ dev, ino uint64 }
	seen := map[namespace]bool{}
	for _, process := range processes {
		if _, err := strconv.ParseUint(process.Name(), 10, 32); err != nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		base := "/proc/" + process.Name()
		var ns unix.Stat_t
		if err := unix.Stat(base+"/ns/mnt", &ns); errors.Is(err, unix.ENOENT) {
			continue
		} else if err != nil {
			return err
		}
		key := namespace{uint64(ns.Dev), ns.Ino}
		if seen[key] {
			continue
		}
		mounts, err := mount.ParseMountInfo(base + "/mountinfo")
		if err != nil || len(mounts) == 0 {
			return errors.Join(devicelease.ErrUncertain, err)
		}
		for _, m := range mounts {
			if int(v.Major) == m.Major && int(v.Minor) == m.Minor {
				return devicelease.ErrHeld
			}
			if !filepath.IsAbs(m.MountPoint) || filepath.Clean(m.MountPoint) != m.MountPoint || strings.Contains(m.MountPoint, `\`) {
				return devicelease.ErrUncertain
			}
			var st unix.Stat_t
			if err := unix.Stat(base+"/root"+m.MountPoint, &st); err != nil {
				return err
			}
			if st.Mode&unix.S_IFMT == unix.S_IFBLK && unix.Major(st.Rdev) == v.Major && unix.Minor(st.Rdev) == v.Minor {
				return devicelease.ErrHeld
			}
		}
		var after unix.Stat_t
		if err := unix.Stat(base+"/ns/mnt", &after); err != nil || after.Dev != ns.Dev || after.Ino != ns.Ino {
			return errors.Join(devicelease.ErrUncertain, err)
		}
		seen[key] = true
	}
	if len(seen) == 0 {
		return devicelease.ErrUncertain
	}
	return nil
}
