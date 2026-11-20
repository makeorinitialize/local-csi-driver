// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package node

import (
	"context"
	"errors"
	"path/filepath"
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

func (v *readOnlyLV) DiscoverOrdinary(_ context.Context, e devicelease.Enrollment) (devicelease.Enrollment, error) {
	e.LVUUID = "lv-uuid"
	return e, nil
}

type candidateReader struct {
	c   devicelease.Candidate
	err error
}

func (r *candidateReader) Resolve(context.Context, string, int64) (devicelease.Candidate, error) {
	return r.c, r.err
}

func TestDynamicEnrollmentAgainstCSIHandlers(t *testing.T) {
	for _, winner := range []string{"native-before-registration", "reserve", "deletion-during-admission", "provision-inflight", "provision-crash"} {
		t.Run(winner, func(t *testing.T) {
			cfg := devicelease.DynamicConfig{NodeName: "node", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "ns-uid",
				CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
			reader := &candidateReader{c: devicelease.Candidate{CompositeID: "composite", CompositeUID: "cr-uid",
				VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc", Fingerprint: "authoritative-journal", RequestID: "reserve"}}
			dir := filepath.Join(t.TempDir(), "dynamic")
			b := &readOnlyLV{}
			open := func() *devicelease.Provider {
				t.Helper()
				p, err := devicelease.OpenDynamic(dir, cfg, reader, b)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := open()
			ctrl := gomock.NewController(t)
			ns := initTestNodeServer(t, ctrl)
			mount := mounter.NewMockMounter(ctrl)
			ns.mounter = mount
			manager := lvmMgr.NewMockManager(ctrl)
			core, err := lvm.New("driver", cfg.NodeName, "ns", false, probe.NewFake(nil, nil), manager,
				telemetry.NewNoopTracerProvider(), lvm.WithOrdinaryHold(open()))
			if err != nil {
				t.Fatal(err)
			}
			ns.volume = core
			req := &csi.NodeUnpublishVolumeRequest{VolumeId: reader.c.VolumeHandle, TargetPath: "/test/dynamic-block"}
			unpublish := func() error { _, err := ns.NodeUnpublishVolume(t.Context(), req); return err }
			reserve := func() (devicelease.Reservation, error) {
				return p.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", reader.c.CompositeID, cfg.Bytes)
			}
			start, proceed := make(chan struct{}), make(chan struct{})
			switch winner {
			case "native-before-registration":
				// The handler has passed its guard, but the actual unmount has
				// NOT completed. No enrollment existed when it was admitted.
				mount.EXPECT().CleanupMountPoint(req.TargetPath).DoAndReturn(func(string) error {
					close(start)
					<-proceed
					return nil
				})
				done := make(chan error, 1)
				go func() { done <- unpublish() }()
				<-start
				_, err := reserve()
				close(proceed)
				if e := <-done; e != nil {
					t.Fatal(e)
				}
				if !errors.Is(err, devicelease.ErrHeld) {
					t.Fatal("adopted under old native command", err)
				}
				p = open()
				if _, err := reserve(); !errors.Is(err, devicelease.ErrHeld) {
					t.Fatal("forgot pre-registration tombstone", err)
				}
				return
			case "deletion-during-admission":
				b.exclusive = func() { reader.err = errors.New("CR deleted") }
				if _, err := reserve(); err == nil {
					t.Fatal("deleted source admitted")
				}
				b.exclusive = nil
				reader.err = nil // even a reappearing source cannot reset uncertainty
				p = open()
				if _, err := reserve(); !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal("reconstructed uncertain enrollment", err)
				}
				if err := unpublish(); status.Code(err) != codes.FailedPrecondition {
					t.Fatal("lost partial admission hold", err)
				}
				return
			case "provision-inflight", "provision-crash":
				manager.EXPECT().GetLogicalVolume(gomock.Any(), cfg.VolumeGroup, "pvc-12345678-1234-1234-1234-123456789abc").
					DoAndReturn(func(context.Context, string, string) (*lvmMgr.LogicalVolume, error) {
						close(start)
						<-proceed
						if winner == "provision-crash" {
							return nil, errors.New("uncertain child command")
						}
						return &lvmMgr.LogicalVolume{VGName: cfg.VolumeGroup, Name: "pvc-12345678-1234-1234-1234-123456789abc", Size: 4096}, nil
					})
				if winner == "provision-inflight" {
					manager.EXPECT().IsLogicalVolumeCorrupted(gomock.Any(), cfg.VolumeGroup, gomock.Any()).Return(false, nil)
				}
				done := make(chan error, 1)
				go func() {
					_, err := core.EnsureVolume(t.Context(), req.VolumeId, cfg.Bytes, cfg.Bytes, false)
					done <- err
				}()
				<-start
				_, err := reserve()
				close(proceed)
				provisionErr := <-done
				if !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal("admitted in-flight provisioning", err)
				}
				p = open()
				if winner == "provision-crash" {
					if provisionErr == nil {
						t.Fatal("missing injected failure")
					}
					if _, err := reserve(); !errors.Is(err, devicelease.ErrUncertain) {
						t.Fatal("forgot crashed provision", err)
					}
					return
				}
				if provisionErr != nil {
					t.Fatal(provisionErr)
				}
			case "reserve":
				b.exclusive = func() { close(start); <-proceed }
				done := make(chan error, 1)
				go func() { _, err := reserve(); done <- err }()
				<-start
				native := make(chan error, 1)
				go func() { native <- unpublish() }()
				close(proceed)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if err := <-native; status.Code(err) != codes.FailedPrecondition {
					t.Fatal("unpublish crossed enrollment commit", err)
				}
				b.exclusive = nil
			}
			r, err := reserve()
			if err != nil {
				t.Fatal(err)
			}
			p = open() // lost reply/restart: the same admission, never another LV
			replay, err := reserve()
			if err != nil || replay != r {
				t.Fatal("lost-reply replay changed allocation", replay, r, err)
			}
			if err := core.Delete(t.Context(), &csi.DeleteVolumeRequest{VolumeId: req.VolumeId}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("LV remove was not fenced", err)
			}
			if err := unpublish(); status.Code(err) != codes.FailedPrecondition {
				t.Fatal(err)
			}
			if _, err := ns.NodeGetVolumeStats(t.Context(), &csi.NodeGetVolumeStatsRequest{VolumeId: req.VolumeId,
				VolumePath: req.TargetPath}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("native device stats opened held LV", err)
			}
			if _, err := ns.NodeUnstageVolume(t.Context(), &csi.NodeUnstageVolumeRequest{VolumeId: req.VolumeId,
				StagingTargetPath: "/test/staged"}); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("native unstage accepted held LV", err)
			}
			if _, err := p.ReserveInPool(t.Context(), cfg.PoolUUID, "second", "another-composite", cfg.Bytes); !errors.Is(err, devicelease.ErrConflict) {
				t.Fatal("multiple volumes accepted", err)
			}
			// Per-allocation scope: unrelated native volumes are not frozen.
			other := &csi.NodeUnpublishVolumeRequest{VolumeId: "containerstorage#other", TargetPath: "/test/other"}
			mount.EXPECT().CleanupMountPoint(other.TargetPath).Return(nil)
			if _, err := ns.NodeUnpublishVolume(t.Context(), other); err != nil {
				t.Fatal("unrelated volume frozen", err)
			}
			if err := p.BeforeGroupRemoval("unrelated-vg"); err != nil {
				t.Fatal("unrelated pool frozen", err)
			}
			reader.err = errors.New("source API unavailable or deleted")
			if _, err := p.Lookup(t.Context(), cfg.PoolUUID, r.LeaseID); err == nil {
				t.Fatal("issued lease against lost source")
			}
			if err := unpublish(); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("source loss released hold", err)
			}
			reader.err = nil
			o := devicelease.Owner{PoolUUID: cfg.PoolUUID, CompositeID: reader.c.CompositeID, LeaseID: r.LeaseID,
				ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, NodeID: cfg.NodeName, Generation: r.Generation, Epoch: 1}
			proof := receipt{o}
			if _, err := p.Acquire(t.Context(), proof, "acquire", o, 0); err != nil {
				t.Fatal(err)
			}
			if err := p.ReleaseNode(t.Context(), proof, "release", o, "guest-clean+host-unplugged"); err != nil {
				t.Fatal(err)
			}
			p = open()
			if _, err := p.ReserveInPool(t.Context(), cfg.PoolUUID, "reuse", o.CompositeID, cfg.Bytes); err == nil {
				t.Fatal("retired slot reused", err)
			}
			mount.EXPECT().CleanupMountPoint(req.TargetPath).Return(nil)
			if err := unpublish(); err != nil {
				t.Fatal("retirement did not permit native teardown", err)
			}
		})
	}
}
