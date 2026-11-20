// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
)

// HostMedia is implemented by the real LV backend. OpenHost must select media
// from provider enrollment and verify it before returning an ordinary FD.
// ConfirmHostUnused holds an exclusive verified probe through durable close.
type HostMedia interface {
	OpenHost(context.Context, Enrollment, Volume) (*os.File, error)
	VerifyHost(*os.File, Enrollment, Volume) error
	ConfirmHostUnused(context.Context, Enrollment, func() error) error
}

func (s *ledger) validateHost() error {
	if s.Dynamic == nil {
		return nil
	}
	d := s.Dynamic
	if d.HostBinding == nil {
		if d.Host != nil {
			return ErrUncertain
		}
		return nil
	}
	if !d.HostBinding.valid() || d.HostBinding.Node != d.Config.NodeName {
		return ErrUncertain
	}
	h := d.Host
	if h == nil {
		return nil
	}
	peer, err := hex.DecodeString(h.Peer)
	r, ok := s.Records[h.Owner.CompositeID]
	if !ok || !h.Owner.valid() || !h.Owner.matches(r) || h.Owner.NodeID != d.Config.NodeName ||
		err != nil || len(peer) != sha256.Size || hex.EncodeToString(peer) != h.Peer || r.Claim == nil ||
		r.Claim.Epoch != h.Owner.Epoch {
		return ErrUncertain
	}
	switch h.Phase {
	case "authorized":
		if h.Activation != (HostActivation{}) || h.Receipt != "" {
			return ErrUncertain
		}
	case "opening", "retained", "cleanup", "closing", "closed":
		o := hostOwner(h.Owner)
		if !h.Activation.valid(*d.HostBinding, o) || h.Activation.Plan.Incarnation != d.Source.CompositeUID ||
			h.Activation.Plan.Grants[1].Bytes != uint64(r.CapacityBytes) || (h.Phase == "closed") != (h.Receipt != "") {
			return ErrUncertain
		}
	default:
		return ErrUncertain
	}
	return nil
}

func hostOwner(o Owner) HostOwner {
	a := HostAllocation{CompositeVolumeID: o.CompositeID, ResourceID: o.ResourceID,
		StableDeviceID: o.StableDeviceID, DeviceGeneration: o.Generation}
	a.Key.Provider, a.Key.LeaseID = o.PoolUUID, o.LeaseID
	return HostOwner{Allocation: a, NodeID: o.NodeID, OwnerEpoch: o.Epoch}
}

// bindHostAuthority is called only while the provider ledger is locked. The
// peer comes from verified mTLS, never the local member's JSON or root UID.
func (p Provider) bindHostAuthority(s *ledger, a Authority, o Owner, prepare bool) error {
	if s.Dynamic == nil || s.Dynamic.HostBinding == nil {
		return nil
	}
	h, ok := a.(*hostAuthority)
	if !ok || p.host == nil || h.member != p.host || h.peer == ([sha256.Size]byte{}) || h.member.stopped.Load() {
		return ErrUncertain
	}
	peer := hex.EncodeToString(h.peer[:])
	if s.Dynamic.Host == nil {
		if !prepare {
			return ErrUncertain
		}
		s.Dynamic.Host = &hostRecord{Owner: o, Peer: peer, Phase: "authorized"}
	}
	r := s.Dynamic.Host
	if r.Owner != o || r.Peer != peer {
		return ErrStale
	}
	h.state = r
	return nil
}

type hostAuthority struct {
	member *HostMember
	peer   [sha256.Size]byte
	state  *hostRecord // one RPC's locked ledger snapshot, never shared
}

func (h *hostAuthority) Enrolled(o Owner) error {
	if h.state == nil || h.state.Owner != o ||
		(h.state.Phase != "authorized" && h.state.Phase != "retained") {
		return ErrHeld
	}
	return h.member.verifyBinding()
}

func (h *hostAuthority) Opened(_ context.Context, o Owner, v Volume) error {
	if h.state == nil || h.state.Owner != o || h.state.Phase != "retained" {
		return ErrHeld
	}
	h.member.mu.Lock()
	defer h.member.mu.Unlock()
	e := h.member.entry
	if e == nil || e.owner != o || e.current == nil || e.original == nil ||
		e.volume.Name != v.Name || e.volume.UUID != v.UUID || e.volume.Bytes != v.Bytes ||
		e.volume.Major != v.Major || e.volume.Minor != v.Minor || !v.Active {
		return ErrUncertain
	}
	if err := sameHostOpen(e.original, e.current); err != nil {
		return err
	}
	return errors.Join(h.member.verifyBinding(), h.member.media.VerifyHost(e.original, e.enrollment, v))
}

func (h *hostAuthority) Closed(o Owner, receipt string) error {
	if h.state == nil || h.state.Owner != o || h.state.Phase != "closed" ||
		receipt == "" || receipt != h.state.Receipt {
		return ErrHeld
	}
	return nil
}

// HostMember cannot be used as an unbound Authority. The RPC server must
// provide the verified client leaf for every node operation.
func (h *HostMember) ForPeer(peer [sha256.Size]byte) Authority {
	return &hostAuthority{member: h, peer: peer}
}
func (*HostMember) Enrolled(Owner) error                        { return ErrUncertain }
func (*HostMember) Opened(context.Context, Owner, Volume) error { return ErrUncertain }
func (*HostMember) Closed(Owner, string) error                  { return ErrUncertain }
