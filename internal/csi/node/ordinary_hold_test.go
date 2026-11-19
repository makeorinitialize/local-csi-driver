// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"local-csi-driver/internal/csi/core/lvm"
	"local-csi-driver/internal/csi/mounter"
	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
	"local-csi-driver/internal/pkg/probe"
	"local-csi-driver/internal/pkg/telemetry"
)

type readOnlyLV struct {
	exclusive            func()
	open                 bool
	probeErr, observeErr error
	target               *devicelease.BlockTarget
}

func (v *readOnlyLV) BlockTarget(string) (*devicelease.BlockTarget, error) { return v.target, nil }

func (v *readOnlyLV) InspectOrdinary(_ context.Context, e devicelease.Enrollment) (devicelease.Volume, error) {
	if v.observeErr != nil {
		return devicelease.Volume{}, v.observeErr
	}
	_, lv, _ := strings.Cut(e.VolumeHandle, "#")
	return devicelease.Volume{Name: lv, UUID: e.LVUUID, Bytes: e.Bytes, Active: true, Open: v.open, Major: 253, Minor: 1}, nil
}
func (v *readOnlyLV) WithExclusive(_ devicelease.Enrollment, _ devicelease.Volume, fn func() error) error {
	if v.exclusive != nil {
		v.exclusive()
	}
	if v.probeErr != nil {
		return v.probeErr
	}
	return fn()
}

type receipt struct{ owner devicelease.Owner }

func (a receipt) Enrolled(o devicelease.Owner) error {
	if a.owner != o {
		return devicelease.ErrHeld
	}
	return nil
}
func (a receipt) Closed(o devicelease.Owner, id string) error {
	if id != "guest-clean+host-unplugged" {
		return devicelease.ErrHeld
	}
	return a.Enrolled(o)
}
func (a receipt) Opened(context.Context, devicelease.Owner, devicelease.Volume) error {
	return devicelease.ErrUncertain
}

func TestOrdinaryBlockHoldOrdersPhysicalUnpublish(t *testing.T) {
	for _, winner := range []string{"hold", "teardown"} {
		t.Run(winner, func(t *testing.T) {
			ctx := t.Context()
			dir := filepath.Join(t.TempDir(), "one-shot")
			e := devicelease.Enrollment{NodeID: "node", CompositeID: "disposable", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc",
				VGUUID: "vg-uuid", LVUUID: "lv-uuid", Generation: 19, VolumeMode: "Block", Bytes: 4096}
			b := &readOnlyLV{}
			if err := devicelease.InitializeOrdinary(ctx, dir, e, b); err != nil {
				t.Fatal(err)
			}
			open := func() *devicelease.Provider {
				p, err := devicelease.OpenOrdinary(dir, e.NodeID, b)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := open()
			ctrl := gomock.NewController(t)
			ns := initTestNodeServer(t, ctrl)
			m := mounter.NewMockMounter(ctrl)
			ns.mounter = m
			setCore := func() {
				core, err := lvm.New("driver", "node", "ns", false, probe.NewFake(nil, nil), lvmMgr.NewMockManager(ctrl), telemetry.NewNoopTracerProvider(), lvm.WithOrdinaryHold(open()))
				if err != nil {
					t.Fatal(err)
				}
				ns.volume = core
			}
			setCore()
			req := &csi.NodeUnpublishVolumeRequest{VolumeId: e.VolumeHandle, TargetPath: "/test/published-block"}
			unpublish := func() error { _, err := ns.NodeUnpublishVolume(ctx, req); return err }
			resolve := func() (devicelease.Reservation, error) {
				return p.ReserveInPool(ctx, e.VGUUID, "resolve", e.CompositeID, e.Bytes)
			}
			journal := func() []byte {
				t.Helper()
				b, err := os.ReadFile(filepath.Join(dir, "ledger.json"))
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			original := journal()
			for _, size := range []int64{e.Bytes - 1, e.Bytes + 1} {
				if _, err := p.ReserveInPool(ctx, e.VGUUID, "resolve", e.CompositeID, size); !errors.Is(err, devicelease.ErrExhausted) {
					t.Fatal(size, err)
				}
			}
			for _, tc := range []struct {
				open           bool
				probeErr, want error
			}{
				{true, nil, devicelease.ErrHeld}, {false, devicelease.ErrHeld, devicelease.ErrHeld}, {false, devicelease.ErrUncertain, devicelease.ErrUncertain},
			} {
				b.open, b.probeErr = tc.open, tc.probeErr
				cap, err := p.Capacity(ctx, e.VGUUID)
				if cap != 0 || (tc.want == devicelease.ErrHeld && err != nil) || (tc.want == devicelease.ErrUncertain && !errors.Is(err, tc.want)) {
					t.Fatal(cap, err)
				}
				if _, err := resolve(); !errors.Is(err, tc.want) {
					t.Fatal("unexpected admission", err)
				}
			}
			b.open, b.probeErr = false, nil
			if !bytes.Equal(original, journal()) {
				t.Fatal("busy, uncertain or size mismatch retained a binding or hold")
			}
			if cap, err := p.Capacity(ctx, e.VGUUID); cap != e.Bytes || err != nil {
				t.Fatal(cap, err)
			}
			start, proceed := make(chan struct{}), make(chan struct{})
			if winner == "teardown" {
				m.EXPECT().CleanupMountPoint(req.TargetPath).DoAndReturn(func(string) error { close(start); <-proceed; return nil })
				done := make(chan error, 1)
				go func() { done <- unpublish() }()
				<-start
				_, err := resolve()
				close(proceed)
				if e := <-done; e != nil {
					t.Fatal(e)
				}
				if !errors.Is(err, devicelease.ErrHeld) {
					t.Fatal("resolve raced an in-flight unmount", err)
				}
				p = open()
				if _, err := resolve(); !errors.Is(err, devicelease.ErrHeld) {
					t.Fatal("lost teardown tombstone", err)
				}
				return
			}
			b.exclusive = func() { close(start); <-proceed }
			var r devicelease.Reservation
			held := make(chan error, 1)
			go func() { var err error; r, err = resolve(); held <- err }()
			<-start
			entered, denial := make(chan struct{}), make(chan error, 1)
			go func() { close(entered); denial <- unpublish() }()
			<-entered
			close(proceed)
			if err := <-held; err != nil {
				t.Fatal(err)
			}
			if err := <-denial; status.Code(err) != codes.FailedPrecondition {
				t.Fatal("unpublish not denied", err)
			}
			b.exclusive = nil
			o := devicelease.Owner{PoolUUID: e.VGUUID, CompositeID: e.CompositeID, LeaseID: r.LeaseID, StableDeviceID: e.LVUUID, ResourceID: r.ResourceID(),
				NodeID: e.NodeID, Generation: e.Generation, Epoch: 1}
			proof := receipt{o}
			if _, err := p.Acquire(ctx, proof, "acquire", o, 0); err != nil {
				t.Fatal(err)
			}
			p = open()
			setCore() // separate RPC and CSI instances, as after restart
			other := &csi.NodeUnpublishVolumeRequest{VolumeId: "containerstorage#other", TargetPath: "/test/stale-unrelated"}
			b.target = &devicelease.BlockTarget{Major: 253, Minor: 1} // vanished target, same dev_t as held owner
			if _, err := ns.NodeUnpublishVolume(ctx, other); status.Code(err) != codes.Unavailable {
				t.Fatal("ambiguous device", err)
			}
			b.target.Minor = 42
			b.observeErr = devicelease.ErrUncertain
			if _, err := ns.NodeUnpublishVolume(ctx, other); status.Code(err) != codes.Unavailable {
				t.Fatal("unknown live identity", err)
			}
			b.observeErr = nil
			m.EXPECT().CleanupMountPoint(other.TargetPath).Return(nil)
			if _, err := ns.NodeUnpublishVolume(ctx, other); err != nil {
				t.Fatal("independently distinct stale target", err)
			}
			b.target = nil
			if _, err := ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: e.VolumeHandle, StagingTargetPath: "/test/staging"}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal(err)
			}
			stale := o
			stale.Generation++
			if err := p.ReleaseNode(ctx, proof, "stale", stale, "guest-clean+host-unplugged"); !errors.Is(err, devicelease.ErrStale) {
				t.Fatal(err)
			}
			if err := p.ReleaseNode(ctx, proof, "release", o, "guest-clean-only"); !errors.Is(err, devicelease.ErrHeld) {
				t.Fatal(err)
			}
			if err := unpublish(); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("released without unplug proof", err)
			}
			if err := p.ReleaseNode(ctx, proof, "release", o, "guest-clean+host-unplugged"); err != nil {
				t.Fatal(err)
			}
			m.EXPECT().CleanupMountPoint(req.TargetPath).Return(nil)
			if err := unpublish(); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Acquire(ctx, proof, "again", o, 0); !errors.Is(err, devicelease.ErrStale) {
				t.Fatal("reacquired retired media", err)
			}
		})
	}
}
