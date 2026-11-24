// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "local-csi-driver/internal/deviceuse/usev1"
)

type fakeResolver struct{ missing bool }

func (r *fakeResolver) CheckScope(_ context.Context, id *pb.UseIdentity) error {
	if r.missing || id.Source.PvUid != "pv-uid" {
		return status.Error(codes.FailedPrecondition, "PV missing or replaced")
	}
	return nil
}
func (r *fakeResolver) Resolve(ctx context.Context, id *pb.UseIdentity) (Resource, error) {
	if err := r.CheckScope(ctx, id); err != nil {
		return Resource{}, err
	}
	return Resource{Key: "actual-vg/lv-uuid", Target: "vg#lv", Group: "vg"}, nil
}

func fixture(t *testing.T) (*Domain, *FileStore, *fakeResolver, *pb.UseIdentity) {
	t.Helper()
	scope := Scope{Driver: OfficialCacheDriver, Namespace: "sources", NamespaceUID: "ns-uid", Node: "node", NodeUID: "node-uid"}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(dir, scope); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir, scope)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &fakeResolver{}
	d := &Domain{Scope: scope, Store: store, Resolver: resolver}
	id := &pb.UseIdentity{UseId: "use-1", CompositeId: "compositedisk-" + strings.Repeat("a", 52) + ".cr-uid", NodeId: "node",
		Source: &pb.SourceVolume{Driver: scope.Driver, VolumeHandle: "vg#lv", PvName: "pv", PvUid: "pv-uid"}}
	return d, store, resolver, id
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("code=%v want=%v err=%v", status.Code(err), code, err)
	}
}

func TestUsePersistsBlocksTeardownAndReleasesWithoutMetadata(t *testing.T) {
	d, store, resolver, id := fixture(t)
	ctx := t.Context()
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	reopened, err := Open(store.path, store.scope)
	requireCode(t, err, codes.OK)
	restarted := &Domain{Scope: d.Scope, Store: reopened, Resolver: resolver}
	for _, method := range []string{"DeleteVolume", "NodeUnpublishVolume", "NodeUnstageVolume", "GC", "LVM"} {
		_, _, err = restarted.BeginNative(ctx, method, "vg#lv", "")
		requireCode(t, err, codes.FailedPrecondition)
	}
	prepareCtx, finish, err := restarted.BeginPreparation(ctx, "NodeStageVolume", "vg#lv", "node", true)
	requireCode(t, err, codes.OK)
	requireCode(t, restarted.CheckPrepare(prepareCtx, "vg#lv"), codes.OK)
	requireCode(t, finish(true), codes.OK)
	resolver.missing = true
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.FailedPrecondition)
	got, err := restarted.GetUse(ctx, &pb.GetUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_ACTIVE {
		t.Fatal("metadata absence cleared hold")
	}
	_, err = restarted.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, finish, err = restarted.BeginNative(ctx, "DeleteVolume", "vg#lv", "")
	requireCode(t, err, codes.OK)
	requireCode(t, finish(true), codes.OK)
}

func TestUseEarlyReleaseIsDurableTerminalAndDoesNotReleaseSuccessor(t *testing.T) {
	d, store, resolver, id := fixture(t)
	ctx := t.Context()
	_, err := d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	reopened, err := Open(store.path, store.scope)
	requireCode(t, err, codes.OK)
	d = &Domain{Scope: d.Scope, Store: reopened, Resolver: resolver}
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.FailedPrecondition)
	next := proto.CloneOf(id)
	next.UseId = "use-2"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	requireCode(t, err, codes.OK)
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, _, err = d.BeginNative(ctx, "NodeUnstageVolume", "vg#lv", "")
	requireCode(t, err, codes.FailedPrecondition)
	mismatch := proto.CloneOf(next)
	mismatch.Source.PvName = "alias"
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: mismatch})
	requireCode(t, err, codes.AlreadyExists)
}

type lostReply struct {
	Store
	lose bool
}

func (s *lostReply) Transact(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	changed := false
	err := s.Store.Transact(ctx, func(state *Snapshot) (bool, error) {
		var callbackErr error
		changed, callbackErr = fn(state)
		return changed, callbackErr
	})
	if err == nil && changed && s.lose {
		s.lose = false
		return errors.New("committed reply lost")
	}
	return err
}

func TestUseNativeAdmissionAndLostCompletionCannotClearSuccessor(t *testing.T) {
	d, store, resolver, id := fixture(t)
	ctx := t.Context()
	loss := &lostReply{Store: store}
	d.Store = loss
	_, finish, err := d.BeginNative(ctx, "NodeUnstageVolume", "vg#lv", "")
	requireCode(t, err, codes.OK)
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	requireCode(t, finish(false), codes.OK)
	reopened, err := Open(store.path, store.scope)
	requireCode(t, err, codes.OK)
	restarted := &Domain{Scope: d.Scope, Store: reopened, Resolver: resolver}
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	loss.lose = true
	requireCode(t, finish(true), codes.OK)
	_, successorFinish, err := d.BeginNative(ctx, "NodeUnpublishVolume", "vg#lv", "")
	requireCode(t, err, codes.OK)
	requireCode(t, finish(true), codes.OK)
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	requireCode(t, successorFinish(true), codes.OK)
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, _, err = d.BeginNative(ctx, "NodeUnstageVolume", "vg#lv", "")
	requireCode(t, err, codes.FailedPrecondition)
}

func TestUseLostAcquireAndReleaseRepliesActualAliasesAndReplacement(t *testing.T) {
	d, store, _, id := fixture(t)
	ctx := t.Context()
	loss := &lostReply{Store: store, lose: true}
	d.Store = loss
	if _, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id}); err == nil {
		t.Fatal("lost acquire reply treated as confirmed")
	}
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	alias := proto.CloneOf(id)
	alias.UseId, alias.Source.PvName, alias.Source.VolumeHandle = "alias-use", "alias", "alias-handle"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: alias})
	requireCode(t, err, codes.AlreadyExists)
	loss.lose = true
	if _, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id}); err == nil {
		t.Fatal("lost release reply treated as confirmed")
	}
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	alias.Source.PvUid = "replacement"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: alias})
	requireCode(t, err, codes.FailedPrecondition)
}

func TestUseConcurrentActualResourceAcquisition(t *testing.T) {
	d, store, resolver, id := fixture(t)
	reopened, err := Open(store.path, store.scope)
	requireCode(t, err, codes.OK)
	other := &Domain{Scope: d.Scope, Store: reopened, Resolver: resolver}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i, domain := range []*Domain{d, other} {
		wg.Add(1)
		go func(i int, domain *Domain) {
			defer wg.Done()
			q := proto.CloneOf(id)
			q.UseId = []string{"first", "second"}[i]
			_, err := domain.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: q})
			results <- err
		}(i, domain)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			requireCode(t, err, codes.AlreadyExists)
		}
	}
	if success != 1 {
		t.Fatalf("actual resource has %d owners", success)
	}
}

func TestKnownNativeSuccessCommitsAfterCallerCancellation(t *testing.T) {
	d, _, _, id := fixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	_, finish, err := d.BeginNative(ctx, "NodeUnpublishVolume", "vg#lv", "")
	requireCode(t, err, codes.OK)
	cancel()
	requireCode(t, finish(true), codes.OK)
	_, err = d.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
}

func TestUseMissingAndIncompatibleJournalsNeverReset(t *testing.T) {
	_, store, _, _ := fixture(t)
	path := filepath.Join(store.path, journalName)
	bad := []byte(`{"Version":5,"Records":{}}`)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(store.path, store.scope); err == nil {
		t.Fatal("incompatible ledger accepted")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(bad) {
		t.Fatal("incompatible ledger rewritten")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(store.path, store.scope); err == nil {
		t.Fatal("missing ledger recreated")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing ledger was recreated")
	}
	if err := os.WriteFile(filepath.Join(store.path, "ledger.json"), bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := Initialize(store.path, store.scope); err == nil {
		t.Fatal("new journal shadowed unresolved legacy state")
	}
}
