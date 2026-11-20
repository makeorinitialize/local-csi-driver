// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

type hostEntry struct {
	owner             Owner
	enrollment        Enrollment
	volume            Volume
	original, current *os.File
	currentToken      uint64
}

// HostMember is the executable provider-owned member. Ledger lock always
// precedes mu. Disconnect only drops the token-matched current duplicate;
// the original and durable provider hold survive facade disconnection.
type HostMember struct {
	db       *Provider
	media    HostMedia
	binding  HostBinding
	listener *net.UnixListener
	uid      uint32
	mu       sync.Mutex
	entry    *hostEntry
	stopped  atomic.Bool
	tokens   atomic.Uint64
}

func CurrentHostBinding(node, stage, pods string) (HostBinding, error) {
	b := HostBinding{Node: node, StageRoot: stage, PodsRoot: pods}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return b, err
	}
	b.BootID = strings.TrimSpace(string(boot))
	st, err := os.Stat("/proc/1/ns/mnt")
	if err != nil {
		return b, err
	}
	s := st.Sys().(*syscall.Stat_t)
	b.MountDevice, b.MountInode = uint64(s.Dev), s.Ino
	if !b.valid() {
		return b, ErrUncertain
	}
	return b, nil
}

func (h *HostMember) verifyBinding() error {
	b, err := CurrentHostBinding(h.binding.Node, h.binding.StageRoot, h.binding.PodsRoot)
	if err != nil || b != h.binding {
		return errors.Join(ErrUncertain, err)
	}
	for _, path := range []string{b.StageRoot, b.PodsRoot} {
		st, err := os.Stat("/proc/1/root" + path)
		if err != nil || !st.IsDir() || st.Mode().Perm()&0022 != 0 || st.Sys().(*syscall.Stat_t).Uid != 0 {
			return errors.Join(ErrUncertain, err)
		}
	}
	// Host PID visibility is needed to independently inspect mount references.
	self, err := os.Stat("/proc/self/ns/pid")
	if err != nil {
		return err
	}
	init, err := os.Stat("/proc/1/ns/pid")
	if err != nil || !os.SameFile(self, init) {
		return errors.Join(ErrUncertain, err)
	}
	return nil
}

func sameHostOpen(a, b *os.File) error {
	if a == nil || b == nil {
		return ErrUncertain
	}
	n, err := unix.FcntlInt(a.Fd(), unix.F_DUPFD_QUERY, int(b.Fd()))
	if err != nil || n != 1 {
		return errors.Join(ErrUncertain, err)
	}
	return nil
}

func hostKernelReady() error {
	fd, err := unix.MemfdCreate("acs-host-ofd-probe", unix.MFD_CLOEXEC)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "acs-host-ofd-probe")
	defer f.Close()
	return sameHostOpen(f, f)
}

// NewHostMember binds a real private listener before RPC startup. Existing
// sockets are not unlinked: restart recovery must not hijack another daemon.
// Configure a private runtime directory whose stale socket is removed only
// after its previous process is independently known stopped.
func NewHostMember(db *Provider, socket string, binding HostBinding) (*HostMember, error) {
	if os.Geteuid() != 0 || db == nil || db.dynamic == nil || !binding.valid() ||
		db.dynamic.config.NodeName != binding.Node || !filepath.IsAbs(socket) ||
		filepath.Clean(socket) != socket || len(socket) > 107 {
		return nil, ErrUncertain
	}
	media, ok := db.observer.(HostMedia)
	if !ok {
		return nil, ErrUncertain
	}
	parent, err := os.Lstat(filepath.Dir(socket))
	if err != nil || !privateDirectory(parent) {
		return nil, errors.Join(ErrUncertain, err)
	}
	h := &HostMember{db: db, media: media, binding: binding, uid: 0}
	if err := h.verifyBinding(); err != nil {
		return nil, err
	}
	if err := hostKernelReady(); err != nil {
		return nil, err
	}
	if err := h.arm(); err != nil {
		return nil, err
	}
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0600); err != nil {
		l.Close()
		return nil, err
	}
	h.listener = l
	db.host = h
	return h, nil
}

func (h *HostMember) arm() error {
	return withLedger(h.db.directory, func(s *ledger, save func() error) error {
		if err := h.db.checkDynamic(s); err != nil {
			return err
		}
		d := s.Dynamic
		if d.HostBinding != nil {
			if *d.HostBinding != h.binding {
				return ErrUncertain
			}
			return nil
		}
		if d.Admission {
			return ErrUncertain
		}
		for _, r := range s.Records {
			if r.Claim != nil {
				return ErrHeld // no conversion of an existing guest/external owner
			}
		}
		d.HostBinding = &h.binding
		return save()
	})
}

func (p Provider) HostConfigured() (bool, error) {
	if p.dynamic == nil {
		return false, nil
	}
	var configured bool
	err := withLedger(p.directory, func(s *ledger, _ func() error) error {
		configured = s.Dynamic != nil && s.Dynamic.HostBinding != nil
		return p.checkDynamic(s)
	})
	return configured, err
}

func (*HostMember) NeedLeaderElection() bool { return false }

func (h *HostMember) disconnect(token uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.entry; e != nil && e.currentToken == token {
		if e.current != nil {
			_ = e.current.Close()
		}
		e.current, e.currentToken = nil, 0
	}
}

func (h *HostMember) handle(ctx context.Context, token uint64, q HostPacket, input *os.File) (HostPacket, *os.File, error) {
	reply := q
	reply.Operation += "-ack"
	cleanup := q.Operation == "retain-host-cleanup" || q.Operation == "bind-host-cleanup" || q.Operation == "check-host-cleanup"
	bind := q.Operation == "bind-host-current" || q.Operation == "bind-host-cleanup"
	if q.Version != HostProtocol || q.Binding != h.binding || q.Nonce == "" ||
		q.Opened != (HostOpened{}) || q.ReceiptKind != "" || q.Receipt != (HostReceipt{}) ||
		bind != (input != nil) || h.stopped.Load() {
		return HostPacket{}, nil, ErrUncertain
	}
	if err := h.verifyBinding(); err != nil {
		return HostPacket{}, nil, err
	}
	var output *os.File
	err := withLedger(h.db.directory, func(s *ledger, save func() error) error {
		if err := h.db.checkDynamic(s); err != nil {
			return err
		}
		d := s.Dynamic
		if d.HostBinding == nil || *d.HostBinding != h.binding {
			return ErrUncertain
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if q.Operation == "ready" {
			want := HostOwner{}
			want.Allocation.Key.Provider = s.PoolUUID
			if q.Owner != want || q.Activation != (HostActivation{}) {
				return ErrUncertain
			}
			if d.Admission || d.Host != nil && (d.Host.Phase == "opening" || d.Host.Phase == "closing" ||
				(d.Host.Phase == "retained" || d.Host.Phase == "cleanup") && h.entry == nil) {
				return ErrUncertain
			}
			return hostKernelReady()
		}
		o := q.Owner.owner()
		r, ok := s.Records[o.CompositeID]
		if !ok || !o.matches(r) || !q.Activation.valid(h.binding, q.Owner) || d.Host == nil ||
			d.Host.Owner != o || q.Activation.Plan.Incarnation != d.Source.CompositeUID ||
			q.Activation.Plan.Grants[1].Bytes != uint64(r.CapacityBytes) {
			return ErrStale
		}
		record := d.Host
		if record.Activation != (HostActivation{}) && record.Activation != q.Activation {
			return ErrStale
		}
		if q.Operation == "finish-host" && record.Phase == "closed" {
			reply.ReceiptKind, reply.Receipt = "native-host-teardown", HostReceipt{Owner: q.Owner, ID: record.Receipt}
			return nil
		}
		c := r.Claim
		if d.Closure != nil || d.Admission || c == nil || c.Node != o.NodeID || c.Epoch != o.Epoch {
			return ErrHeld
		}
		if q.Operation == "finish-host" {
			if c.Phase != "acquired" && c.Phase != "held" {
				return ErrHeld
			}
			// Block all future retain/bind before dropping any provider FD.
			// A crash or failed proof remains closing, never a new original.
			if record.Phase != "closing" {
				record.Phase, record.Activation = "closing", q.Activation
				if err := save(); err != nil {
					return err
				}
			}
			if e := h.entry; e != nil {
				if e.current != nil {
					_ = e.current.Close()
				}
				if e.original != nil {
					_ = e.original.Close()
				}
				h.entry = nil
			}
			// With our originals closed, kernel open counts include borrowed
			// FDs and queued SCM_RIGHTS. No timeout or socket absence is proof.
			return h.media.ConfirmHostUnused(ctx, *s.Canary, func() error {
				record.Phase, record.Receipt = "closed", "native-host-"+uuid.NewString()
				if err := save(); err != nil {
					return err
				}
				reply.ReceiptKind, reply.Receipt = "native-host-teardown", HostReceipt{Owner: q.Owner, ID: record.Receipt}
				return nil
			})
		}
		if cleanup {
			if (c.Phase != "acquired" && c.Phase != "held") || (record.Phase != "retained" && record.Phase != "cleanup") ||
				h.entry == nil || h.entry.original == nil || h.entry.owner != o {
				return ErrHeld // no opening or reconstructing a cleanup descriptor
			}
			if q.Operation != "retain-host-cleanup" && record.Phase != "cleanup" {
				return ErrHeld
			}
			v, err := h.db.inventoryFor(ctx, s, r, true)
			if err != nil || !live(c, v) {
				return errors.Join(ErrUncertain, err)
			}
			if err := h.media.VerifyHost(h.entry.original, *s.Canary, v); err != nil {
				return err
			}
			if record.Phase != "cleanup" {
				record.Phase = "cleanup"
				if err := save(); err != nil {
					return err
				}
			}
			if q.Operation == "retain-host-cleanup" {
				fd, err := unix.FcntlInt(h.entry.original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
				if err != nil {
					return err
				}
				output = os.NewFile(uintptr(fd), "acs-host-cleanup")
				reply.Opened = HostOpened{StableDeviceID: v.UUID, DeviceGeneration: r.Generation,
					CapacityBytes: uint64(v.Bytes), ObservedMajor: v.Major, ObservedMinor: v.Minor}
				return nil
			}
			if bind {
				if err := sameHostOpen(h.entry.original, input); err != nil {
					return err
				}
				fd, err := unix.FcntlInt(input.Fd(), unix.F_DUPFD_CLOEXEC, 0)
				if err != nil {
					return err
				}
				if h.entry.current != nil {
					_ = h.entry.current.Close()
				}
				h.entry.current = os.NewFile(uintptr(fd), "acs-host-cleanup-current")
				h.entry.currentToken = token
			}
			if h.entry.currentToken != token || h.entry.current == nil {
				return ErrHeld
			}
			return sameHostOpen(h.entry.original, h.entry.current)
		}
		if c.Phase != "acquired" || record.Phase == "closed" || record.Phase == "closing" {
			return ErrHeld
		}
		if record.Phase == "cleanup" {
			return ErrHeld
		}
		if err := beforeDeadline(c, save); err != nil {
			return err
		}
		v, err := h.db.inventory(ctx, s, r)
		if err != nil || !live(c, v) {
			return errors.Join(ErrUncertain, err)
		}
		switch q.Operation {
		case "retain-host":
			if record.Phase == "authorized" {
				record.Phase, record.Activation = "opening", q.Activation
				if err := save(); err != nil {
					return err
				}
				f, err := h.media.OpenHost(ctx, *s.Canary, v)
				if err != nil {
					return err
				}
				h.entry = &hostEntry{owner: o, enrollment: *s.Canary, volume: v, original: f}
				record.Phase = "retained"
				if err := save(); err != nil {
					return err
				}
			}
			if record.Phase != "retained" || h.entry == nil || h.entry.owner != o {
				return ErrUncertain // never reconstruct a lost original
			}
			if err := h.media.VerifyHost(h.entry.original, *s.Canary, v); err != nil {
				return err
			}
			fd, err := unix.FcntlInt(h.entry.original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
			if err != nil {
				return err
			}
			output = os.NewFile(uintptr(fd), "acs-host-borrow")
			reply.Opened = HostOpened{StableDeviceID: v.UUID, DeviceGeneration: r.Generation,
				CapacityBytes: uint64(v.Bytes), ObservedMajor: v.Major, ObservedMinor: v.Minor}
			return nil
		case "bind-host-current", "check-host":
			e := h.entry
			if record.Phase != "retained" || e == nil || e.owner != o {
				return ErrUncertain
			}
			if q.Operation == "bind-host-current" {
				if err := sameHostOpen(e.original, input); err != nil {
					return err
				}
				fd, err := unix.FcntlInt(input.Fd(), unix.F_DUPFD_CLOEXEC, 0)
				if err != nil {
					return err
				}
				if e.current != nil {
					_ = e.current.Close()
				}
				e.current, e.currentToken = os.NewFile(uintptr(fd), "acs-host-current"), token
			}
			if e.currentToken != token || e.current == nil {
				return ErrHeld
			}
			if err := sameHostOpen(e.original, e.current); err != nil {
				return err
			}
			return h.media.VerifyHost(e.original, *s.Canary, v)
		default:
			return ErrUncertain
		}
	})
	if err != nil {
		if output != nil {
			_ = output.Close()
		}
		return HostPacket{}, nil, err
	}
	return reply, output, nil
}
