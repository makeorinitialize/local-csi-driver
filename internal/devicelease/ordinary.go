// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

const OrdinaryPool = "acs-disposable-block"

// Enrollment is an offline operator attestation, not a path supplied by a
// workload. Only one already provisioned disposable Block PV per node.
type Enrollment struct {
	NodeID, CompositeID, VolumeHandle, VGUUID, LVUUID, VolumeMode string
	Generation                                                    uint64
	Bytes                                                         int64
}

var pvcLV = regexp.MustCompile(`^pvc-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (e Enrollment) valid() bool {
	vg, lv, ok := strings.Cut(e.VolumeHandle, "#")
	return ok && OrdinaryTarget(vg, lv) && pvcLV.MatchString(lv) && e.NodeID != "" && e.CompositeID != "" && e.VGUUID != "" && e.LVUUID != "" && e.Generation > 0 && e.Bytes > 0 && e.VolumeMode == "Block"
}

type OrdinaryObserver interface {
	InspectOrdinary(context.Context, Enrollment) (Volume, error)
	WithExclusive(Enrollment, Volume, func() error) error
	BlockTarget(string) (*BlockTarget, error)
}

// An empty DMUUID requires independent live enrolled-device evidence.
type BlockTarget struct {
	Major, Minor uint32
	DMUUID       string
}

// InitializeOrdinary only reads media. Directory creation is exclusive: a
// missing/corrupt journal inside an existing directory is never bootstrapped.
func InitializeOrdinary(ctx context.Context, path string, e Enrollment, b OrdinaryObserver) error {
	if !e.valid() || b == nil {
		return errors.New("invalid ordinary Block enrollment")
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := unix.Flock(int(parent.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("enroll only while the driver on this node is stopped")
	}
	v, err := observe(ctx, b, e)
	if err != nil {
		return err
	}
	return withUnused(b, e, v, func() error {
		return publishEnrollment(parent, path, func(dir *os.File) error {
			return saveLedger(dir, &ledger{Version: 3, PoolUUID: e.VGUUID, Canary: &e,
				Records: map[string]Reservation{}, Requests: map[string]reserveIntent{}})
		})
	})
}

// Only the fully synced journal is discoverable. A crash before rename can
// leave a private sibling, never an empty enrollment at the startup path.
func publishEnrollment(parent *os.File, path string, write func(*os.File) error) error {
	staging, err := os.MkdirTemp(parent.Name(), ".pending-block-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	if err := locked(staging, write); err != nil {
		return err
	}
	if err := unix.Renameat2(int(parent.Fd()), filepath.Base(staging), int(parent.Fd()), filepath.Base(path), unix.RENAME_NOREPLACE); err != nil {
		return err
	}
	return parent.Sync()
}

func withUnused(b OrdinaryObserver, e Enrollment, v Volume, fn func() error) error {
	if v.Open {
		return ErrHeld
	}
	return b.WithExclusive(e, v, fn)
}

// Pin the startup decision: an older running instance of this binary that
// observed no enrollment must exit before offline enrollment can succeed.
func PinEnrollment(path string) (func(), error) {
	return pinEnrollment(path, false)
}

// PinDynamicEnrollment excludes older driver instances that pinned an absent
// enrollment, as well as duplicate online providers. This is a startup-only
// domain: never enable it behind already running CSI handlers.
func PinDynamicEnrollment(path string) (func(), error) {
	return pinEnrollment(path, true)
}

func pinEnrollment(path string, dynamic bool) (func(), error) {
	parent, err := os.Open(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) && !dynamic {
		return func() {}, nil
	} // non-chart installation, no canary mount
	if err != nil {
		return nil, err
	}
	mode := unix.LOCK_SH
	if dynamic {
		mode = unix.LOCK_EX
	}
	if err := unix.Flock(int(parent.Fd()), mode|unix.LOCK_NB); err != nil {
		_ = parent.Close()
		return nil, err
	}
	return func() { _ = parent.Close() }, nil
}

// OpenOrdinary is called even with the RPC listener disabled.
func OpenOrdinary(path, node string, b OrdinaryObserver) (*Provider, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !privateDirectory(info) || b == nil {
		return nil, ErrUncertain
	}
	p := &Provider{directory: path, observer: b}
	err = withLedger(path, func(s *ledger, _ func() error) error {
		if s.Dynamic != nil || s.Canary == nil || s.Canary.NodeID != node {
			return ErrUncertain
		}
		p.ordinary = s.Canary
		return nil
	})
	return p, err
}

func observe(ctx context.Context, b OrdinaryObserver, e Enrollment) (Volume, error) {
	v, err := b.InspectOrdinary(ctx, e)
	_, name, _ := strings.Cut(e.VolumeHandle, "#")
	if err != nil || v.Name != name || v.UUID != e.LVUUID || v.Bytes != e.Bytes || !v.Active {
		return Volume{}, errors.Join(ErrUncertain, err)
	}
	return v, nil
}
func (p *Provider) PoolID() string {
	if p.isOrdinary() {
		return OrdinaryPool
	}
	return Group
}
func (r Reservation) ResourceID() string {
	if r.Handle != "" {
		return strings.Replace(r.Handle, "#", "/", 1)
	}
	return Group + "/" + r.name()
}

func (p *Provider) checkCanary(s *ledger) error {
	if p.dynamic != nil {
		if err := p.checkDynamic(s); err != nil {
			return err
		}
		if s.Canary == nil || s.Dynamic.Admission {
			return ErrUncertain
		}
		return nil
	}
	if s.Canary == nil || *s.Canary != *p.ordinary {
		return ErrUncertain
	}
	return nil
}

func (p *Provider) ordinaryQuery(ctx context.Context, pool, request, composite, id string, n int64) (Reservation, int64, error) {
	var result Reservation
	var available int64
	err := withLedger(p.directory, func(s *ledger, save func() error) error {
		var err error
		result, available, err = p.queryOrdinary(ctx, s, save, pool, request, composite, id, n)
		return err
	})
	return result, available, err
}

func (p *Provider) queryOrdinary(ctx context.Context, s *ledger, save func() error, pool, request, composite, id string, n int64) (Reservation, int64, error) {
	var result Reservation
	var available int64
	err := func() error {
		if err := p.checkCanary(s); err != nil {
			return err
		}
		e := p.ordinary
		if pool != e.VGUUID {
			return ErrStale
		}
		r, exists := s.Records[e.CompositeID]
		if s.Retired {
			if request == "" && id == "" {
				return nil
			}
			// A committed node release permits CSI teardown, not another
			// acquisition. Historical Resolve/GetLease still require live media.
			if !exists || request != "" && s.Requests[request].CompositeID == "" {
				return ErrHeld
			}
		}
		v, err := observe(ctx, p.observer, *e)
		if err != nil {
			return err
		}
		if request == "" && id == "" {
			if !exists {
				err := withUnused(p.observer, *e, v, func() error { available = v.Bytes; return nil })
				if errors.Is(err, ErrHeld) {
					return nil
				}
				return err
			}
			return nil
		}
		if request != "" {
			if composite != e.CompositeID {
				return ErrConflict
			}
			input := reserveIntent{composite, n}
			old, known := s.Requests[request]
			if _, ok := s.Mutations[request]; ok || known && old != input || exists && r.RequestedBytes != n {
				return ErrConflict
			}
			if !exists {
				if n != e.Bytes {
					return ErrExhausted
				}
				var key [16]byte
				if _, err := rand.Read(key[:]); err != nil {
					return err
				}
				r = Reservation{PoolUUID: pool, CompositeID: composite, LeaseID: hex.EncodeToString(key[:]), StableDeviceID: e.LVUUID,
					Handle: e.VolumeHandle, Generation: e.Generation, RequestedBytes: n, CapacityBytes: e.Bytes}
				// Do not admit the request or publish the hold unless the exact
				// unused device is still exclusively open through journal sync.
				err := withUnused(p.observer, *e, v, func() error {
					if err := ctx.Err(); err != nil {
						return err
					}
					s.Records[composite], s.Requests[request] = r, input
					return save()
				})
				if err != nil {
					return err
				}
				result = r
				return nil
			}
			if !known {
				s.Requests[request] = input
				if err := save(); err != nil {
					return err
				}
			}
		} else if !exists || r.LeaseID != id {
			return ErrNotFound
		}
		if err := snapshot(r, v, save); err != nil {
			return err
		}
		result = r
		return nil
	}()
	if err != nil {
		return Reservation{}, 0, err
	}
	return result, available, nil
}

// BeforePhysical is a durable linearization point, not a check followed by
// an unlocked race: retiring the one-shot enrollment precludes every future
// resolve/acquire, even if the physical command crashes or is still running.
func (p *Provider) BeforePhysical(id, lvUUID string) error {
	if p != nil && p.dynamic != nil {
		if !p.inDomain(id) && lvUUID == "" {
			return nil
		}
		return p.dynamicPhysical(id, lvUUID, nil)
	}
	if p == nil || p.ordinary == nil {
		return nil
	}
	e := p.ordinary
	vg, lv, _ := strings.Cut(e.VolumeHandle, "#")
	mapper := "mapper#" + strings.ReplaceAll(vg, "-", "--") + "-" + strings.ReplaceAll(lv, "-", "--")
	if id != e.VolumeHandle && id != mapper && lvUUID != e.LVUUID {
		return nil
	}
	return withLedger(p.directory, func(s *ledger, save func() error) error {
		if err := p.checkCanary(s); err != nil {
			return err
		}
		if s.Retired {
			return nil
		}
		if len(s.Records) != 0 {
			return ErrHeld
		}
		s.Retired = true
		return save()
	})
}

func (p *Provider) BeforePoolRemoval() error {
	if p != nil && p.dynamic != nil {
		return p.dynamicPhysical("pool", "", nil)
	}
	if p == nil || p.ordinary == nil {
		return nil
	}
	return p.BeforePhysical(p.ordinary.VolumeHandle, "")
}

// CleanupMountPoint acts on the target, not on the CSI handle. Also protect
// a bind-mounted block inode if an inconsistent handle names another volume.
// The path is never used to enroll, resolve, open or grant access.
func (p *Provider) BeforeUnpublish(id, path string) error {
	if p != nil && p.dynamic != nil {
		target, err := p.observer.BlockTarget(path)
		if err != nil {
			return err
		}
		if !p.inDomain(id) && (target == nil || target.DMUUID != "" &&
			!strings.HasPrefix(target.DMUUID, "LVM-"+strings.ReplaceAll(p.dynamic.config.PoolUUID, "-", ""))) {
			return nil
		}
		return p.dynamicPhysical(id, "", target)
	}
	if p == nil || p.ordinary == nil {
		return nil
	}
	if err := p.BeforePhysical(id, ""); err != nil {
		return err
	}
	if id == p.ordinary.VolumeHandle {
		return nil
	}
	target, err := p.observer.BlockTarget(path)
	if err != nil || target == nil {
		return err
	}
	e := p.ordinary
	if target.DMUUID == "LVM-"+strings.ReplaceAll(e.VGUUID+e.LVUUID, "-", "") {
		return p.BeforePhysical(e.VolumeHandle, "")
	}
	if target.DMUUID != "" {
		return nil
	}
	// ENOENT in sysfs alone identifies nothing. Check a stale bind inode
	// against the held incarnation AND the independently verified live LV.
	return withLedger(p.directory, func(s *ledger, _ func() error) error {
		if err := p.checkCanary(s); err != nil {
			return err
		}
		if s.Retired {
			return nil
		}
		v, err := observe(context.Background(), p.observer, *e)
		if err != nil {
			return err
		}
		if v.Major == target.Major && v.Minor == target.Minor {
			return ErrUncertain
		}
		if c := s.Records[e.CompositeID].Claim; c != nil && !live(c, v) {
			return ErrUncertain
		}
		return nil
	})
}
