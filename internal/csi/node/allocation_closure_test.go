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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"local-csi-driver/internal/csi/controller"
	"local-csi-driver/internal/csi/core/lvm"
	"local-csi-driver/internal/csi/mounter"
	"local-csi-driver/internal/devicelease"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
	"local-csi-driver/internal/pkg/probe"
	"local-csi-driver/internal/pkg/telemetry"
)

type blockedAcquisition struct {
	receipt
	entered, proceed chan struct{}
}

func (a blockedAcquisition) Enrolled(o devicelease.Owner) error {
	close(a.entered)
	<-a.proceed
	return a.receipt.Enrolled(o)
}

func TestAllocationClosureOrdersActualCSIReclaim(t *testing.T) {
	for _, scenario := range []string{"reserved", "acquire-wins", "close-wins", "verified-node-release", "uncertain-enrollment", "uncertain-acquisition"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := devicelease.DynamicConfig{NodeName: "node", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "ns-uid",
				CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
			source := &candidateReader{c: devicelease.Candidate{CompositeID: "composite", CompositeUID: "cr-uid", RequestID: "reserve",
				VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc", Fingerprint: "full-journal"}}
			b := &readOnlyLV{}
			path := filepath.Join(t.TempDir(), "closure")
			open := func() *devicelease.Provider {
				t.Helper()
				p, err := devicelease.OpenDynamic(path, cfg, source, b)
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := open()
			ctrl := gomock.NewController(t)
			ns := initTestNodeServer(t, ctrl)
			mount := mounter.NewMockMounter(ctrl)
			manager := lvmMgr.NewMockManager(ctrl)
			core, err := lvm.New("driver", cfg.NodeName, "ns", false, probe.NewFake(nil, nil), manager,
				telemetry.NewNoopTracerProvider(), lvm.WithOrdinaryHold(open()), lvm.WithVolumeWipeEnabled(false))
			if err != nil {
				t.Fatal(err)
			}
			ns.volume, ns.mounter = core, mount
			pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-12345678-1234-1234-1234-123456789abc"},
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: "localdisk.csi.acstor.io", VolumeHandle: source.c.VolumeHandle}}}}
			if err := ns.k8sClient.Create(t.Context(), pv); err != nil {
				t.Fatal(err)
			}
			cs := controller.New(core, core.GetControllerDriverCapabilities(), core.GetNodeAccessModes(), mount, ns.k8sClient,
				cfg.NodeName, nil, "", "", false, ns.recorder, telemetry.NewNoopTracerProvider())
			deleteRequest := &csi.DeleteVolumeRequest{VolumeId: source.c.VolumeHandle}
			deleteVolume := func() error { _, err := cs.DeleteVolume(t.Context(), deleteRequest); return err }
			reserve := func() (devicelease.Reservation, error) {
				return p.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", source.c.CompositeID, cfg.Bytes)
			}
			if scenario == "uncertain-enrollment" {
				b.probeErr = devicelease.ErrUncertain
			}
			r, err := reserve()
			if scenario == "uncertain-enrollment" {
				if !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal(err)
				}
				guess := devicelease.Allocation{PoolUUID: cfg.PoolUUID, CompositeID: source.c.CompositeID,
					LeaseID: "00000000000000000000000000000000", ResourceID: "containerstorage/pvc-12345678-1234-1234-1234-123456789abc",
					StableDeviceID: "lv-uuid", Generation: 1}
				p = open()
				if err := p.ReleaseAllocation(t.Context(), "close", guess, 0); !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal("absence of receipt closed uncertain admission", err)
				}
				if err := deleteVolume(); status.Code(err) != codes.FailedPrecondition {
					t.Fatal("uncertain admission allowed native delete", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			a := devicelease.Allocation{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID,
				ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation}
			o := devicelease.Owner{PoolUUID: a.PoolUUID, CompositeID: a.CompositeID, LeaseID: a.LeaseID,
				ResourceID: a.ResourceID, StableDeviceID: a.StableDeviceID, Generation: a.Generation, NodeID: cfg.NodeName, Epoch: 1}
			proof := receipt{o}
			if err := deleteVolume(); status.Code(err) != codes.FailedPrecondition {
				t.Fatal("reservation did not hold actual CSI DeleteVolume", err)
			}
			if scenario == "uncertain-acquisition" {
				b.probeErr = devicelease.ErrUncertain
				if _, err := p.Acquire(t.Context(), proof, "acquire", o, 0); !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal(err)
				}
				b.probeErr = nil
				p = open()
				for _, epoch := range []uint64{0, o.Epoch} {
					if err := p.ReleaseAllocation(t.Context(), "close", a, epoch); err == nil {
						t.Fatal("missing acquisition receipt authorized closure", epoch)
					}
				}
				if _, err := p.Lookup(t.Context(), cfg.PoolUUID, a.LeaseID); !errors.Is(err, devicelease.ErrUncertain) {
					t.Fatal("uncertain acquisition advertised as Reserved", err)
				}
				if err := deleteVolume(); status.Code(err) != codes.FailedPrecondition {
					t.Fatal("uncertain acquisition allowed native reclaim", err)
				}
				return
			}
			epoch := uint64(0)
			switch scenario {
			case "acquire-wins":
				entered, proceed := make(chan struct{}), make(chan struct{})
				acquired, closed := make(chan error, 1), make(chan error, 1)
				go func() {
					_, err := p.Acquire(t.Context(), blockedAcquisition{proof, entered, proceed}, "acquire", o, 0)
					acquired <- err
				}()
				<-entered // Acquire owns the ledger lock, but has not committed.
				closeEntered := make(chan struct{})
				go func() { close(closeEntered); closed <- p.ReleaseAllocation(t.Context(), "close", a, 0) }()
				<-closeEntered
				close(proceed)
				if err := <-acquired; err != nil {
					t.Fatal(err)
				}
				if err := <-closed; err == nil {
					t.Fatal("close raced an admitted acquisition")
				}
				fallthrough
			case "verified-node-release":
				if scenario == "verified-node-release" {
					if _, err := p.Acquire(t.Context(), proof, "acquire", o, 0); err != nil {
						t.Fatal(err)
					}
				}
				if err := p.ReleaseAllocation(t.Context(), "close", a, o.Epoch); !errors.Is(err, devicelease.ErrHeld) {
					t.Fatal("acquired owner closed without verified release", err)
				}
				if err := p.ReleaseNode(t.Context(), proof, "release", o, "guest-clean+host-unplugged"); err != nil {
					t.Fatal(err)
				}
				epoch = o.Epoch
				if err := p.ReleaseAllocation(t.Context(), "close", a, 0); !errors.Is(err, devicelease.ErrStale) {
					t.Fatal("released epoch was not pinned", err)
				}
				if err := p.ReleaseAllocation(t.Context(), "release", a, epoch); !errors.Is(err, devicelease.ErrConflict) {
					t.Fatal("closure overwrote node-release request history", err)
				}
				if err := deleteVolume(); status.Code(err) != codes.FailedPrecondition {
					t.Fatal("node release alone allowed physical delete", err)
				}
			}
			if err := p.ReleaseAllocation(t.Context(), "close", a, epoch); err != nil {
				t.Fatal(err)
			}
			// A late/new/replayed owner cannot reopen the authorization.
			if _, err := p.Acquire(t.Context(), proof, "acquire", o, 0); err == nil {
				t.Fatal("late acquisition crossed closed tombstone")
			}
			if _, err := reserve(); err == nil {
				t.Fatal("old Reserve replay reopened the slot")
			}
			manager.EXPECT().GetLogicalVolume(gomock.Any(), cfg.VolumeGroup, "pvc-12345678-1234-1234-1234-123456789abc").
				Return(&lvmMgr.LogicalVolume{VGName: cfg.VolumeGroup, Name: "pvc-12345678-1234-1234-1234-123456789abc", Size: 4096}, nil)
			manager.EXPECT().IsLogicalVolumeCorrupted(gomock.Any(), cfg.VolumeGroup, gomock.Any()).Return(false, nil)
			if _, err := core.EnsureVolume(t.Context(), deleteRequest.VolumeId, cfg.Bytes, cfg.Bytes, true); err != nil {
				t.Fatal("ordinary source lifecycle remained held after composite closure", err)
			}
			// Only the real native CSI handler invokes cleanup and lvremove.
			manager.EXPECT().RemoveLogicalVolume(gomock.Any(), lvmMgr.RemoveLVOptions{Name: a.ResourceID}).Return(nil)
			if scenario == "close-wins" {
				entered, proceed := make(chan struct{}), make(chan struct{})
				mount.EXPECT().CleanupStagingDir(gomock.Any(), "/dev/"+a.ResourceID).DoAndReturn(func(context.Context, string) error {
					close(entered)
					<-proceed
					return nil
				})
				deleted := make(chan error, 1)
				go func() { deleted <- deleteVolume() }()
				<-entered
				_, acquireErr := p.Acquire(t.Context(), proof, "late", o, 0)
				_, reserveErr := reserve()
				close(proceed)
				if err := <-deleted; err != nil {
					t.Fatal(err)
				}
				if acquireErr == nil || reserveErr == nil {
					t.Fatal("late authorization crossed in-flight native delete", acquireErr, reserveErr)
				}
			} else {
				mount.EXPECT().CleanupStagingDir(gomock.Any(), "/dev/"+a.ResourceID).Return(nil)
				if err := deleteVolume(); err != nil {
					t.Fatal("closed authorization blocked native reclamation", err)
				}
			}
			source.err, b.observeErr = errors.New("source removed"), errors.New("LV removed")
			p = open()
			if err := p.ReleaseAllocation(context.Background(), "close", a, epoch); err != nil {
				t.Fatal("lost close reply required resurrecting source/media", err)
			}
			if _, err := p.Lookup(t.Context(), cfg.PoolUUID, a.LeaseID); !errors.Is(err, devicelease.ErrNotFound) {
				t.Fatal("closed key no longer authoritative after deletion", err)
			}
			if _, err := p.Lookup(t.Context(), cfg.PoolUUID, "unknown"); !errors.Is(err, devicelease.ErrUncertain) {
				t.Fatal("unknown key reported closed", err)
			}
			if err := p.ReleaseAllocation(t.Context(), "changed-request", a, epoch); !errors.Is(err, devicelease.ErrConflict) {
				t.Fatal("new request rewrote closure history", err)
			}
		})
	}
}
