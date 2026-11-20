// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"path/filepath"

	"github.com/google/uuid"
)

const HostProtocol = "dmstack-host-ofd-v1"

// Exact independent copy of the host member contract. This is not a new
// DeviceProvider API and has no dependency on the private facade Go module.
type HostBinding struct {
	Node                    string
	BootID                  string
	MountDevice, MountInode uint64
	StageRoot, PodsRoot     string
}

type HostAllocation struct {
	Key struct {
		Provider, LeaseID string
	}
	CompositeVolumeID, ResourceID, StableDeviceID string
	DeviceGeneration                              uint64
}

type HostOwner struct {
	Allocation HostAllocation
	NodeID     string
	OwnerEpoch uint64
}

func (o HostOwner) owner() Owner {
	a := o.Allocation
	return Owner{PoolUUID: a.Key.Provider, LeaseID: a.Key.LeaseID, CompositeID: a.CompositeVolumeID,
		ResourceID: a.ResourceID, StableDeviceID: a.StableDeviceID, Generation: a.DeviceGeneration,
		NodeID: o.NodeID, Epoch: o.OwnerEpoch}
}

type HostGrant struct {
	Allocation HostAllocation
	Pool       string
	Bytes      uint64
}

type HostActivation struct {
	Plan struct {
		Volume, Incarnation, Node, Backend string
		Grants                             [2]HostGrant
	}
	ID, Stage string
	Epoch     uint64
}

type HostOpened struct {
	StableDeviceID               string
	DeviceGeneration             uint64
	CapacityBytes                uint64
	ObservedMajor, ObservedMinor uint32
}

type HostReceipt struct {
	Owner HostOwner
	ID    string
}

type HostPacket struct {
	Version, Operation, Nonce string
	Binding                   HostBinding
	Activation                HostActivation
	Owner                     HostOwner
	Opened                    HostOpened
	ReceiptKind               string
	Receipt                   HostReceipt
}

func (b HostBinding) valid() bool {
	return b.Node != "" && b.BootID != "" && b.MountInode != 0 &&
		filepath.IsAbs(b.StageRoot) && filepath.Clean(b.StageRoot) == b.StageRoot && b.StageRoot != "/" &&
		filepath.IsAbs(b.PodsRoot) && filepath.Clean(b.PodsRoot) == b.PodsRoot && b.PodsRoot != "/" &&
		b.StageRoot != b.PodsRoot
}

func (a HostActivation) valid(b HostBinding, o HostOwner) bool {
	id, err := uuid.Parse(a.ID)
	if err != nil || id == uuid.Nil || id.String() != a.ID || !o.owner().valid() ||
		a.Plan.Backend != "host" || a.Plan.Node != b.Node || a.Plan.Node != o.NodeID ||
		a.Plan.Volume != o.Allocation.CompositeVolumeID || a.Plan.Incarnation == "" || a.Epoch != o.OwnerEpoch ||
		!filepath.IsAbs(a.Stage) || filepath.Clean(a.Stage) != a.Stage ||
		filepath.Dir(filepath.Dir(a.Stage)) != b.StageRoot || filepath.Base(a.Stage) != "globalmount" {
		return false
	}
	for _, g := range a.Plan.Grants {
		if !(HostOwner{Allocation: g.Allocation, NodeID: o.NodeID, OwnerEpoch: o.OwnerEpoch}).owner().valid() ||
			g.Allocation.CompositeVolumeID != a.Plan.Volume || g.Pool == "" || g.Bytes == 0 {
			return false
		}
	}
	return a.Plan.Grants[1].Allocation == o.Allocation &&
		a.Plan.Grants[1].Pool == OrdinaryPool &&
		a.Plan.Grants[0].Allocation.Key.Provider != a.Plan.Grants[1].Allocation.Key.Provider &&
		a.Plan.Grants[0].Allocation.StableDeviceID != a.Plan.Grants[1].Allocation.StableDeviceID
}

type hostRecord struct {
	Owner      Owner
	Peer       string // SHA256 of the independently verified Acquire client leaf
	Phase      string
	Activation HostActivation
	Receipt    string
}
