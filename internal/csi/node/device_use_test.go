// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/csi/core"
	"local-csi-driver/internal/csi/mounter"
	"local-csi-driver/internal/deviceuse"
	pb "local-csi-driver/internal/deviceuse/usev1"
)

type testUseSource struct{}

func (testUseSource) CheckScope(context.Context, *pb.UseIdentity) error { return nil }
func (testUseSource) Resolve(context.Context, *pb.UseIdentity) (deviceuse.Resource, error) {
	return deviceuse.Resource{Key: "real-native-uuid", Target: testVolumeID, Group: "vg"}, nil
}

type useVolume struct {
	core.NodeInterface
	domain *deviceuse.Domain
}

func (v useVolume) BeginNative(ctx context.Context, method, id, group string) (context.Context, func(bool) error, error) {
	return v.domain.BeginNative(ctx, method, id, group)
}
func (v useVolume) BeginPreparation(ctx context.Context, method, id, node string, block bool) (context.Context, func(bool) error, error) {
	return v.domain.BeginPreparation(ctx, method, id, node, block)
}

func useNode(t *testing.T) (*Server, *deviceuse.Domain, *pb.UseIdentity, string) {
	t.Helper()
	ns := initTestNodeServer(t, gomock.NewController(t))
	scope := deviceuse.Scope{Driver: driverName, Namespace: "sources", NamespaceUID: "namespace-uid", Node: testNodeName, NodeUID: "node-uid"}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := deviceuse.Initialize(dir, scope); err != nil {
		t.Fatal(err)
	}
	store, err := deviceuse.Open(dir, scope)
	if err != nil {
		t.Fatal(err)
	}
	d := &deviceuse.Domain{Scope: scope, Store: store, Resolver: testUseSource{}}
	ns.volume.(*core.Fake).BaseDir = t.TempDir()
	ns.volume = useVolume{ns.volume, d}
	id := &pb.UseIdentity{UseId: "first", NodeId: testNodeName, CompositeId: "compositedisk-" + strings.Repeat("a", 52) + ".cr",
		Source: &pb.SourceVolume{Driver: driverName, VolumeHandle: testVolumeID, PvName: "pv", PvUid: "pv-uid"}}
	return ns, d, id, dir
}

func checkUseCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got %v, want %v: %v", status.Code(err), code, err)
	}
}

func TestDeviceUseAllowsNativeBlockPrepareAndBlocksTeardown(t *testing.T) {
	ns, d, id, _ := useNode(t)
	ctx := t.Context()
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}, AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}
	target := filepath.Join(t.TempDir(), "block")
	stage := t.TempDir()
	_, err = ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: testVolumeID, StagingTargetPath: stage, VolumeCapability: capability})
	checkUseCode(t, err, codes.OK)
	mounter := ns.mounter.(*mounter.MockMounter)
	mounter.EXPECT().IsLikelyNotMountPoint(filepath.Dir(target)).Return(true, nil)
	mounter.EXPECT().FileExists(target).Return(true, nil)
	source, err := ns.volume.GetNodeDevicePath(testVolumeID)
	checkUseCode(t, err, codes.OK)
	mounter.EXPECT().Mount(source, target, "", []string{"bind"}).Return(nil)
	_, err = ns.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId: testVolumeID, StagingTargetPath: stage, TargetPath: target, VolumeCapability: capability})
	checkUseCode(t, err, codes.OK)
	_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.FailedPrecondition)
	_, err = ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: testVolumeID, StagingTargetPath: stage})
	checkUseCode(t, err, codes.FailedPrecondition)
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	mounter.EXPECT().CleanupMountPoint(target).Return(nil)
	_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.OK)
}

func TestDeviceUseNativeTeardownAndNewAcquireAreAtomic(t *testing.T) {
	ns, d, id, _ := useNode(t)
	ctx := t.Context()
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	next := proto.CloneOf(id)
	next.UseId = "successor"
	started, proceed := make(chan struct{}), make(chan struct{})
	defer close(proceed)
	target := filepath.Join(t.TempDir(), "block")
	ns.mounter.(*mounter.MockMounter).EXPECT().CleanupMountPoint(target).DoAndReturn(func(string) error {
		close(started)
		<-proceed
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native teardown did not start")
	}
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	checkUseCode(t, err, codes.Unavailable)
	proceed <- struct{}{}
	checkUseCode(t, <-done, codes.OK)
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	checkUseCode(t, err, codes.OK)
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.FailedPrecondition)
}

func TestDeviceUseUnknownNativeTeardownSurvivesProcessRestart(t *testing.T) {
	ns, d, id, dir := useNode(t)
	ctx := t.Context()
	target := filepath.Join(t.TempDir(), "block")
	ns.mounter.(*mounter.MockMounter).EXPECT().CleanupMountPoint(target).Return(errors.New("native outcome unknown"))
	_, err := ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.Internal)
	store, err := deviceuse.Open(dir, d.Scope)
	checkUseCode(t, err, codes.OK)
	restart := &deviceuse.Domain{Scope: d.Scope, Store: store, Resolver: testUseSource{}}
	_, err = restart.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	checkUseCode(t, err, codes.Unavailable)
}

func TestDeviceUseLatePublishCannotRaceSuccessfulUnpublish(t *testing.T) {
	ns, d, id, _ := useNode(t)
	ctx := t.Context()
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	target := filepath.Join(t.TempDir(), "block")
	mounts := ns.mounter.(*mounter.MockMounter)
	mounts.EXPECT().IsLikelyNotMountPoint(filepath.Dir(target)).Return(true, nil)
	mounts.EXPECT().FileExists(target).Return(true, nil)
	source, err := ns.volume.GetNodeDevicePath(testVolumeID)
	checkUseCode(t, err, codes.OK)
	started, proceed := make(chan struct{}), make(chan struct{})
	defer close(proceed)
	mounts.EXPECT().Mount(source, target, "", []string{"bind"}).DoAndReturn(func(string, string, string, []string) error {
		close(started)
		<-proceed
		return nil
	})
	publishCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ns.NodePublishVolume(publishCtx, &csi.NodePublishVolumeRequest{
			VolumeId: testVolumeID, StagingTargetPath: filepath.Dir(target), TargetPath: target,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}, AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("native publication did not start")
	}
	cancel() // The caller lost its reply; the remote native mount is still running.
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	checkUseCode(t, err, codes.OK)
	_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.Unavailable)
	proceed <- struct{}{}
	checkUseCode(t, <-done, codes.OK)
	mounts.EXPECT().CleanupMountPoint(target).Return(nil)
	_, err = ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	checkUseCode(t, err, codes.OK)
}
