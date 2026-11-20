// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"local-csi-driver/internal/devicelease"
	pb "local-csi-driver/internal/devicelease/v1"
)

type capturedPeerAuthority struct {
	*enrolled
	seen chan [sha256.Size]byte
}

func (a *capturedPeerAuthority) ForPeer(peer [sha256.Size]byte) devicelease.Authority {
	a.seen <- peer
	return a.enrolled
}

func TestHostAuthorityReceivesVerifiedAcquireLeaf(t *testing.T) {
	cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
		CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
		Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
	db, err := devicelease.OpenDynamic(filepath.Join(t.TempDir(), "peer"), cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", "one", cfg.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	o := &pb.Owner{Allocation: lease(r).Allocation, NodeId: "n1", OwnerEpoch: 1}
	expected, err := owner(o)
	if err != nil {
		t.Fatal(err)
	}
	a := &capturedPeerAuthority{enrolled: &enrolled{owner: expected}, seen: make(chan [sha256.Size]byte, 1)}
	const node = "spiffe://test/node/n1"
	clients, leaves := authenticatedTLSClients(t, policy{Provider: cfg.PoolUUID, LocalCustody: true,
		Grants: []Grant{{URI: node, NodeID: "n1", Role: "node", Composites: []string{"one"}}}}, db, a)
	c := clients(node)
	if _, err := c.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire", Owner: o}); err != nil {
		t.Fatal(err)
	}
	got := <-a.seen
	if hex.EncodeToString(got[:]) != (*leaves)[0] {
		t.Fatal("member peer was not independently obtained from verified mTLS")
	}
}

func (canaryObservation) DiscoverOrdinary(_ context.Context, e devicelease.Enrollment) (devicelease.Enrollment, error) {
	e.LVUUID = "observed-lv-uuid"
	return e, nil
}

type canarySource struct {
	mu       sync.Mutex
	c        devicelease.Candidate
	deleting bool
}

func (r *canarySource) Resolve(context.Context, string, int64) (devicelease.Candidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleting {
		return devicelease.Candidate{}, devicelease.ErrUncertain
	}
	return r.c, nil
}
func TestDynamicRPCReservedReceiptBeforeAttachment(t *testing.T) {
	cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
		CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
		Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
	path := filepath.Join(t.TempDir(), "online")
	db, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	const ctl, node, other = "spiffe://test/controller/n1", "spiffe://test/node/n1", "spiffe://test/other/n1"
	policy := policy{Provider: cfg.PoolUUID, LocalCustody: true, Grants: []Grant{
		{URI: ctl, NodeID: cfg.NodeName, Role: "controller", Composites: []string{"one"}},
		{URI: node, NodeID: cfg.NodeName, Role: "node", Composites: []string{"one"}},
		{URI: other, NodeID: cfg.NodeName, Role: "controller", Composites: []string{"other"}},
	}}
	clients := authenticatedClients(t, policy, db, new(enrolled))
	controller, consumer := clients(ctl), clients(node)
	if _, err := controller.GetLease(t.Context(), &pb.LeaseKey{Provider: cfg.PoolUUID, LeaseId: "unknown"}); status.Code(err) != codes.Unavailable {
		t.Fatal("unknown key reported authoritative absence", err)
	}
	q := &pb.ReserveRequest{RequestId: "reserve", CompositeVolumeId: "one", CapacityBytes: uint64(cfg.Bytes),
		Pool: &pb.Pool{Provider: cfg.PoolUUID, PoolId: devicelease.OrdinaryPool}}
	if _, err := consumer.Reserve(t.Context(), q); status.Code(err) != codes.PermissionDenied {
		t.Fatal("node principal enrolled media", err)
	}
	q.RequestId = "not-the-spec-request"
	if _, err := controller.Reserve(t.Context(), q); status.Code(err) != codes.AlreadyExists {
		t.Fatal("unbound source request admitted", err)
	}
	q.RequestId = "reserve"
	reserved, err := controller.Reserve(t.Context(), q)
	if err != nil || reserved.GetState() != pb.LeaseState_LEASE_STATE_RESERVED || reserved.ObservedDevicePath != "" ||
		reserved.Allocation.StableDeviceId != "observed-lv-uuid" || reserved.Allocation.Key.Provider != cfg.PoolUUID ||
		reserved.PoolId != devicelease.OrdinaryPool || reserved.HoldPolicy != pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE ||
		reserved.RenewByUnixSeconds != 0 {
		t.Fatal("Reserve did not return real pre-attach identity", reserved, err)
	}
	got, err := controller.GetLease(t.Context(), reserved.Allocation.Key)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("pre-attach receipt not discoverable", got, err)
	}
	// DeleteHandle can begin deletion before recovering a lost Reserve reply.
	source.mu.Lock()
	source.deleting = true
	source.mu.Unlock()
	got, err = controller.Reserve(t.Context(), q)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("deletion stranded committed receipt", got, err)
	}
	got, err = controller.GetLease(t.Context(), reserved.Allocation.Key)
	if err != nil || !proto.Equal(got, reserved) {
		t.Fatal("deleting source lost committed receipt", got, err)
	}
	if _, err := consumer.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire",
		Owner: &pb.Owner{Allocation: reserved.Allocation, NodeId: cfg.NodeName, OwnerEpoch: 1}}); status.Code(err) != codes.Unavailable {
		t.Fatal("receipt recovery enabled node access to deleting source", err)
	}
	source.mu.Lock()
	source.c.CompositeUID = "replacement"
	source.mu.Unlock()
	if got, err := controller.GetLease(t.Context(), reserved.Allocation.Key); err != nil || !proto.Equal(got, reserved) {
		t.Fatal("observation did not preserve original committed identity", got, err)
	}
	closeRequest := &pb.ReleaseRequest{RequestId: "release-reserve-0", Allocation: reserved.Allocation,
		Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION}
	if _, err := consumer.Release(t.Context(), closeRequest); status.Code(err) != codes.PermissionDenied {
		t.Fatal("node principal closed allocation", err)
	}
	for _, mutate := range []func(*pb.ReleaseRequest){
		func(q *pb.ReleaseRequest) { q.OwnerEpoch = 1 },
		func(q *pb.ReleaseRequest) { q.Allocation.DeviceGeneration++ },
		func(q *pb.ReleaseRequest) { q.Allocation.StableDeviceId += "-changed" },
		func(q *pb.ReleaseRequest) { q.Allocation.ResourceId = "/dev/" + q.Allocation.ResourceId },
		func(q *pb.ReleaseRequest) { q.Allocation.Key.LeaseId += "-changed" },
	} {
		wrong := proto.Clone(closeRequest).(*pb.ReleaseRequest)
		mutate(wrong)
		if _, err := controller.Release(t.Context(), wrong); status.Code(err) != codes.Aborted {
			t.Fatal("inexact closure accepted", wrong, err)
		}
	}
	conflict := proto.Clone(closeRequest).(*pb.ReleaseRequest)
	conflict.RequestId = "reserve"
	if _, err := controller.Release(t.Context(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatal("closure overwrote Reserve request history", err)
	}
	if _, err := controller.Release(t.Context(), closeRequest); err != nil {
		t.Fatal("committed never-acquired allocation could not close", err)
	}
	// Restart after a lost close reply. Source identity is no longer available;
	// only the exact durable tombstone can acknowledge this key as closed.
	reopened, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{})
	if err != nil {
		t.Fatal(err)
	}
	restarted := authenticatedClients(t, policy, reopened, new(enrolled))
	controller, consumer = restarted(ctl), restarted(node)
	if _, err := controller.Release(t.Context(), closeRequest); err != nil {
		t.Fatal("exact closure replay depended on live source", err)
	}
	if _, err := controller.GetLease(t.Context(), reserved.Allocation.Key); status.Code(err) != codes.NotFound {
		t.Fatal("closed key lost authoritative absence", err)
	}
	if _, err := restarted(other).GetLease(t.Context(), reserved.Allocation.Key); status.Code(err) != codes.PermissionDenied {
		t.Fatal("closed-key observation bypassed composite authorization", err)
	}
	if _, err := controller.GetLease(t.Context(), &pb.LeaseKey{Provider: cfg.PoolUUID, LeaseId: "unknown"}); status.Code(err) != codes.Unavailable {
		t.Fatal("unknown key confused with closed key", err)
	}
	if _, err := controller.Reserve(t.Context(), q); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("late Reserve replay reopened closed authorization", err)
	}
	if _, err := consumer.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire",
		Owner: &pb.Owner{Allocation: reserved.Allocation, NodeId: cfg.NodeName, OwnerEpoch: 1}}); status.Code(err) != codes.Aborted {
		t.Fatal("late Acquire reopened closed authorization", err)
	}
	conflict = proto.Clone(closeRequest).(*pb.ReleaseRequest)
	conflict.OwnerEpoch++
	if _, err := controller.Release(t.Context(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatal("closure replay changed committed epoch", err)
	}
}

type durableObservation struct {
	canaryObservation
	open atomic.Bool
}

func (m *durableObservation) InspectOrdinary(ctx context.Context, e devicelease.Enrollment) (devicelease.Volume, error) {
	v, err := m.canaryObservation.InspectOrdinary(ctx, e)
	v.Open = m.open.Load()
	return v, err
}

func (m *durableObservation) WithExclusive(_ devicelease.Enrollment, _ devicelease.Volume, fn func() error) error {
	if m.open.Load() {
		return devicelease.ErrHeld
	}
	return fn()
}

type durableAuthority struct {
	*enrolled
	closed atomic.Bool
}

func (a *durableAuthority) Closed(o devicelease.Owner, receipt string) error {
	if a.closed.Load() && o == a.owner && receipt == "independent-cleanup" {
		return nil
	}
	return devicelease.ErrHeld
}

// The fake clock crosses the former one-minute lease lifetime without sleeping
// in real time. Transport authentication, protobufs and the durable journal are
// real; only source/media/independent cleanup evidence are fixtures.
func TestDynamicRPCExplicitReleaseSurvivesClockAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
			CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
		source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
			Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
		path := filepath.Join(t.TempDir(), "durable")
		media := &durableObservation{}
		db, err := devicelease.OpenDynamic(path, cfg, source, media)
		if err != nil {
			t.Fatal(err)
		}
		// Reserve RPC coverage (including policy on RESERVED) is above. Set the
		// independent authority's immutable owner before exposing this listener.
		r, err := db.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", "one", cfg.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		o := &pb.Owner{Allocation: lease(r).Allocation, NodeId: "n1", OwnerEpoch: 1}
		expected, err := owner(o)
		if err != nil {
			t.Fatal(err)
		}
		proof := &durableAuthority{enrolled: &enrolled{owner: expected}}
		const ctl, node = "spiffe://test/controller/n1", "spiffe://test/node/n1"
		p := policy{Provider: cfg.PoolUUID, LocalCustody: true, Grants: []Grant{
			{URI: ctl, NodeID: "n1", Role: "controller", Composites: []string{"one"}},
			{URI: node, NodeID: "n1", Role: "node", Composites: []string{"one"}},
		}}
		clients := authenticatedClients(t, p, db, proof)
		consumer, controller := clients(node), clients(ctl)
		acquire := &pb.AcquireRequest{RequestId: "acquire", Owner: o}
		acquired, err := consumer.Acquire(t.Context(), acquire)
		if err != nil || acquired.GetState() != pb.LeaseState_LEASE_STATE_ACQUIRED ||
			acquired.GetHoldPolicy() != pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE || acquired.RenewByUnixSeconds != 0 {
			t.Fatal("explicit acquisition", acquired, err)
		}
		readJournal := func() []byte {
			t.Helper()
			b, err := os.ReadFile(filepath.Join(path, "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		committed := readJournal()
		media.open.Store(true)
		validate := &pb.ValidateRequest{Owner: o, Opened: &pb.OpenedDevice{
			StableDeviceId: acquired.Allocation.StableDeviceId, DeviceGeneration: acquired.Allocation.DeviceGeneration,
			CapacityBytes: acquired.CapacityBytes, ObservedMajor: acquired.ObservedMajor, ObservedMinor: acquired.ObservedMinor}}
		time.Sleep(2 * time.Minute)
		if got, err := consumer.GetLease(t.Context(), o.Allocation.Key); err != nil || !proto.Equal(got, acquired) {
			t.Fatal("clock changed authorization", got, err)
		}
		if _, err := consumer.Validate(t.Context(), validate); err != nil {
			t.Fatal("clock ended current owner authorization", err)
		}
		db, err = devicelease.OpenDynamic(path, cfg, source, media)
		if err != nil {
			t.Fatal(err)
		}
		clients = authenticatedClients(t, p, db, proof)
		consumer, controller = clients(node), clients(ctl)
		if got, err := consumer.Acquire(t.Context(), acquire); err != nil || !proto.Equal(got, acquired) {
			t.Fatal("restart lost exact acquisition replay", got, err)
		}
		if _, err := consumer.Validate(t.Context(), validate); err != nil {
			t.Fatal("restart lost current owner recheck", err)
		}
		// Current-source checks still apply to replay; durable does not mean
		// accepting a replacement CR or a caller's old device hints.
		source.mu.Lock()
		source.c.CompositeUID = "replacement"
		source.mu.Unlock()
		if _, err := consumer.Validate(t.Context(), validate); status.Code(err) != codes.Unavailable {
			t.Fatal("source replacement accepted after restart", err)
		}
		source.mu.Lock()
		source.c.CompositeUID = "cr-uid"
		source.mu.Unlock()
		wrong := proto.Clone(validate).(*pb.ValidateRequest)
		wrong.Owner.OwnerEpoch++
		if _, err := consumer.Validate(t.Context(), wrong); status.Code(err) != codes.Aborted {
			t.Fatal("stale epoch accepted", err)
		}
		if _, err := consumer.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "takeover", Owner: wrong.Owner, PreviousOwnerEpoch: 1}); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("new acquire took over durable owner", err)
		}
		if _, err := consumer.Renew(t.Context(), &pb.RenewRequest{RequestId: "renew", Owner: o}); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("explicit-release renewal accepted", err)
		}
		if !bytes.Equal(committed, readJournal()) {
			t.Fatal("clock/recheck/rejected renewal mutated durable ownership")
		}
		assertNativeBlocked := func() {
			t.Helper()
			if !errors.Is(db.BeforePhysical(r.Handle, r.StableDeviceID), devicelease.ErrHeld) ||
				!errors.Is(db.BeforeUnpublish(r.Handle, "/fixture-target"), devicelease.ErrHeld) {
				t.Fatal("native delete/unpublish passed before explicit authorization closure")
			}
		}
		assertNativeBlocked()
		closeRequest := &pb.ReleaseRequest{RequestId: "close", Allocation: o.Allocation, OwnerEpoch: 1, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION}
		if _, err := controller.Release(t.Context(), closeRequest); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("allocation closed without node cleanup", err)
		}
		release := &pb.ReleaseRequest{RequestId: "release", Allocation: o.Allocation, OwnerEpoch: 1,
			NodeId: "n1", Mode: pb.ReleaseMode_RELEASE_MODE_NODE, CleanupReceiptId: "independent-cleanup"}
		if _, err := consumer.Release(t.Context(), release); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("unverified cleanup accepted", err)
		}
		proof.closed.Store(true)
		if _, err := consumer.Release(t.Context(), release); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("live media bypassed exclusive release probe", err)
		}
		media.open.Store(false)
		badRelease := proto.Clone(release).(*pb.ReleaseRequest)
		badRelease.OwnerEpoch++
		if _, err := consumer.Release(t.Context(), badRelease); status.Code(err) != codes.Aborted {
			t.Fatal("wrong epoch released owner", err)
		}
		if _, err := consumer.Release(t.Context(), release); err != nil {
			t.Fatal("verified release failed", err)
		}
		assertNativeBlocked()
		if _, err := controller.Release(t.Context(), closeRequest); err != nil {
			t.Fatal("verified authorization closure failed", err)
		}
		if err := db.BeforePhysical(r.Handle, r.StableDeviceID); err != nil {
			t.Fatal("native lifecycle not restored after explicit closure", err)
		}
		if err := db.BeforeUnpublish(r.Handle, "/fixture-target"); err != nil {
			t.Fatal(err)
		}
		if _, err := consumer.GetLease(t.Context(), o.Allocation.Key); status.Code(err) != codes.NotFound {
			t.Fatal("closed authorization remained active", err)
		}
	})
}

func TestDynamicRPCRejectsLegacyAndMixedPolicyWithoutRewriting(t *testing.T) {
	for _, mode := range []string{"legacy-reserved", "legacy-acquired", "legacy-held", "missing-policy", "unknown-policy", "mixed-policy", "timed-explicit"} {
		t.Run(mode, func(t *testing.T) {
			cfg := devicelease.DynamicConfig{NodeName: "n1", NodeUID: "node-uid", Namespace: "backing", NamespaceUID: "namespace-uid",
				CacheClass: "cache", CacheClassUID: "class-uid", VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
			source := &canarySource{c: devicelease.Candidate{CompositeID: "one", CompositeUID: "cr-uid", RequestID: "reserve",
				Fingerprint: "full-journal", VolumeHandle: "containerstorage#pvc-12345678-1234-1234-1234-123456789abc"}}
			path := filepath.Join(t.TempDir(), "quarantined")
			db, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{})
			if err != nil {
				t.Fatal(err)
			}
			r, err := db.ReserveInPool(t.Context(), cfg.PoolUUID, "reserve", "one", cfg.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			o := &pb.Owner{Allocation: lease(r).Allocation, NodeId: "n1", OwnerEpoch: 1}
			expected, _ := owner(o)
			a := &enrolled{owner: expected}
			const node = "spiffe://test/node/n1"
			p := policy{Provider: cfg.PoolUUID, LocalCustody: true, Grants: []Grant{
				{URI: node, NodeID: "n1", Role: "node", Composites: []string{"one"}},
			}}
			c := authenticatedClients(t, p, db, a)(node)
			if mode != "legacy-reserved" {
				if _, err := c.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire", Owner: o}); err != nil {
					t.Fatal(err)
				}
			}
			journal := filepath.Join(path, "ledger.json")
			data, err := os.ReadFile(journal)
			if err != nil {
				t.Fatal(err)
			}
			var state map[string]any
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			record := state["Records"].(map[string]any)["one"].(map[string]any)
			switch mode {
			case "legacy-reserved", "legacy-acquired", "legacy-held":
				state["Version"] = 4
				delete(state, "HoldPolicy")
				delete(record, "HoldPolicy")
				if mode != "legacy-reserved" {
					claim := record["Claim"].(map[string]any)
					claim["Deadline"] = 1 // already expired legacy authorization
					if mode == "legacy-held" {
						claim["Phase"] = "held"
					}
				}
			case "missing-policy":
				delete(state, "HoldPolicy")
			case "unknown-policy":
				state["HoldPolicy"] = 2
			case "mixed-policy":
				delete(record, "HoldPolicy")
			case "timed-explicit":
				record["Claim"].(map[string]any)["Deadline"] = 1
			}
			old, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(journal, old, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := devicelease.OpenDynamic(path, cfg, source, canaryObservation{}); !errors.Is(err, devicelease.ErrUncertain) {
				t.Fatal("old/ambiguous state silently authorized at startup", err)
			}
			if _, err := c.GetLease(t.Context(), o.Allocation.Key); status.Code(err) != codes.Unavailable {
				t.Fatal("old/ambiguous state exposed as durable lease", err)
			}
			if _, err := c.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire", Owner: o}); status.Code(err) != codes.Unavailable {
				t.Fatal("old/ambiguous owner reauthorized", err)
			}
			if _, err := c.Renew(t.Context(), &pb.RenewRequest{RequestId: "renew", Owner: o}); status.Code(err) != codes.Unavailable {
				t.Fatal("quarantine renewed", err)
			}
			if _, err := c.Validate(t.Context(), &pb.ValidateRequest{Owner: o, Opened: &pb.OpenedDevice{
				StableDeviceId: r.StableDeviceID, DeviceGeneration: r.Generation, CapacityBytes: uint64(r.CapacityBytes),
				ObservedMajor: 253, ObservedMinor: 1}}); status.Code(err) != codes.Unavailable {
				t.Fatal("quarantine authorized current IO", err)
			}
			if _, err := c.Release(t.Context(), &pb.ReleaseRequest{RequestId: "release", Allocation: o.Allocation, OwnerEpoch: 1,
				NodeId: "n1", Mode: pb.ReleaseMode_RELEASE_MODE_NODE, CleanupReceiptId: "caller-assertion"}); status.Code(err) != codes.Unavailable {
				t.Fatal("quarantine released ownership", err)
			}
			if db.BeforePhysical(r.Handle, r.StableDeviceID) == nil || db.BeforeUnpublish(r.Handle, "/fixture-target") == nil {
				t.Fatal("quarantine permitted native reclaim")
			}
			got, err := os.ReadFile(journal)
			if err != nil || !bytes.Equal(old, got) {
				t.Fatal("quarantine rewrote protected historical evidence", err)
			}
		})
	}
}
