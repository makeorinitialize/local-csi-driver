// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"

	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

var lvmID = regexp.MustCompile(`^[A-Za-z0-9]{6}(-[A-Za-z0-9]{4}){5}-[A-Za-z0-9]{6}$`)
var leaseID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func change(r devicelease.Reservation, activate *lvmMgr.YesNo) (lvmMgr.UpdateLVOptions, error) {
	if !leaseID.MatchString(r.LeaseID) || !lvmID.MatchString(r.StableDeviceID) || !lvmID.MatchString(r.PoolUUID) || r.Generation == 0 {
		return lvmMgr.UpdateLVOptions{}, devicelease.ErrUncertain
	}
	return lvmMgr.UpdateLVOptions{Name: devicelease.Group + "/raw-" + r.LeaseID, Activate: activate,
		Select:               fmt.Sprintf("vg_uuid=%s && lv_uuid=%s && lv_tags=acs.dmstack.gen.%d && lv_tags=acs.dmstack.lease.%s", r.PoolUUID, r.StableDeviceID, r.Generation, r.LeaseID),
		IgnoreActivationSkip: true}, nil
}
func (b Backend) Activate(ctx context.Context, r devicelease.Reservation) error {
	opt, err := change(r, lvmMgr.Yes)
	if err != nil {
		return err
	}
	return b.Manager.UpdateLogicalVolume(ctx, opt)
}

func sysdev(v devicelease.Volume) string {
	return fmt.Sprintf("/sys/dev/block/%d:%d", v.Major, v.Minor)
}

// VerifyDescriptor operates on an already-open file description. Neither
// the caller's path nor metadata echoed from a lease is evidence of identity.
func VerifyDescriptor(fd int, vg, lv string, v devicelease.Volume) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFBLK || unix.Major(st.Rdev) != v.Major || unix.Minor(st.Rdev) != v.Minor {
		return devicelease.ErrUncertain
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_ACCMODE != unix.O_RDWR {
		return devicelease.ErrUncertain
	}
	dm, err := os.ReadFile(sysdev(v) + "/dm/uuid")
	if err != nil {
		return err
	}
	if !lvmID.MatchString(vg) || !lvmID.MatchString(lv) || strings.TrimSpace(string(dm)) != "LVM-"+strings.ReplaceAll(vg+lv, "-", "") {
		return devicelease.ErrUncertain
	}
	var n uint64
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&n))); errno != 0 {
		return errno
	}
	if n != uint64(v.Bytes) || v.Bytes <= 0 {
		return devicelease.ErrUncertain
	}
	return nil
}

func noHolders(v devicelease.Volume) error {
	entries, err := os.ReadDir(sysdev(v) + "/holders")
	if err != nil {
		return err
	}
	if v.Open || len(entries) != 0 {
		return devicelease.ErrHeld
	}
	return nil
}
func (b Backend) Prepare(ctx context.Context, r devicelease.Reservation, v devicelease.Volume, first bool) error {
	if _, err := change(r, lvmMgr.Yes); err != nil {
		return err
	}
	if err := noHolders(v); err != nil {
		return err
	}
	fd, err := unix.Open("/dev/"+devicelease.Group+"/raw-"+r.LeaseID, unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := VerifyDescriptor(fd, r.PoolUUID, r.StableDeviceID, v); err != nil {
		return err
	}
	if !first {
		return nil
	}
	// First disclosure only. Zero and flush this exact verified descriptor,
	// never reopen by name or discard data on a later handoff.
	zeros := make([]byte, 1<<20)
	for off := int64(0); off < v.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := unix.Pwrite(fd, zeros[:min(int64(len(zeros)), v.Bytes-off)], off)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("short device write")
		}
		off += int64(n)
	}
	return unix.Fsync(fd)
}
func (b Backend) Deactivate(ctx context.Context, r devicelease.Reservation, v devicelease.Volume) error {
	opt, err := change(r, lvmMgr.No)
	if err != nil {
		return err
	}
	if err := noHolders(v); err != nil {
		return err
	}
	// No force: the kernel must also allow removal of this exact mapping.
	return b.Manager.UpdateLogicalVolume(ctx, opt)
}

func (b Backend) ConfirmInactive(ctx context.Context, r devicelease.Reservation) error {
	// Inactive LVM metadata alone does not exclude a lingering kernel map.
	var fs unix.Statfs_t
	if err := unix.Statfs("/sys/block", &fs); err != nil {
		return err
	}
	if fs.Type != unix.SYSFS_MAGIC {
		return devicelease.ErrUncertain
	}
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return err
	}
	uuid := "LVM-" + strings.ReplaceAll(r.PoolUUID+r.StableDeviceID, "-", "")
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "dm-") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		dm, err := os.ReadFile("/sys/block/" + e.Name() + "/dm/uuid")
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(dm)) == uuid {
			return devicelease.ErrHeld
		}
	}
	return nil
}
