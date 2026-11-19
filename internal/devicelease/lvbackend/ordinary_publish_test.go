// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package lvbackend

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
	mount "k8s.io/mount-utils"

	"local-csi-driver/internal/devicelease"
)

func TestUnpublishedChecksKubeletBindInode(t *testing.T) {
	v := devicelease.Volume{Major: 253, Minor: 4}
	target := "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/vol/pod"
	mounts := []mount.MountInfo{{MountPoint: target, Major: 0, Minor: 5}}
	for _, tc := range []struct {
		minor         uint32
		statErr, want error
	}{{4, nil, devicelease.ErrHeld}, {5, nil, nil}, {0, unix.ENOENT, unix.ENOENT}} {
		err := unpublished(v, mounts, func(path string, st *unix.Stat_t) error {
			if path != "/proc/1/root"+target {
				t.Fatal(path)
			}
			st.Mode, st.Dev, st.Rdev = unix.S_IFBLK, unix.Mkdev(0, 5), unix.Mkdev(253, tc.minor)
			return tc.statErr
		})
		if !errors.Is(err, tc.want) {
			t.Fatalf("minor %d: %v", tc.minor, err)
		}
	}
}
