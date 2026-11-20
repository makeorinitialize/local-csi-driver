// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Regular-file media only: real SCM_RIGHTS/OFD/kernel tests never open a host
// block device. Production selection/verification is lvbackend.Backend.
type hostFixtureMedia struct {
	path          string
	stat          unix.Stat_t
	opens         atomic.Int32
	wrongIdentity atomic.Bool
}

func (m *hostFixtureMedia) count() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	n := 0
	for _, entry := range entries {
		var st unix.Stat_t
		if unix.Stat("/proc/self/fd/"+entry.Name(), &st) == nil && st.Dev == m.stat.Dev && st.Ino == m.stat.Ino {
			n++
		}
	}
	return n
}
func (*hostFixtureMedia) BlockTarget(string) (*BlockTarget, error) { return nil, nil }
func (*hostFixtureMedia) DiscoverOrdinary(_ context.Context, e Enrollment) (Enrollment, error) {
	e.LVUUID = "fixture-lv"
	return e, nil
}
func (m *hostFixtureMedia) InspectOrdinary(_ context.Context, e Enrollment) (Volume, error) {
	_, name, _ := strings.Cut(e.VolumeHandle, "#")
	id := e.LVUUID
	if m.wrongIdentity.Load() {
		id = "different-lv"
	}
	return Volume{Name: name, UUID: id, Bytes: e.Bytes, Active: true, Open: m.count() != 0, Major: 253, Minor: 9}, nil
}
func (m *hostFixtureMedia) WithExclusive(_ Enrollment, _ Volume, fn func() error) error {
	if m.count() != 0 {
		return ErrHeld
	}
	return fn()
}
func (m *hostFixtureMedia) OpenHost(_ context.Context, _ Enrollment, _ Volume) (*os.File, error) {
	m.opens.Add(1)
	return os.OpenFile(m.path, os.O_RDWR, 0)
}
func (m *hostFixtureMedia) VerifyHost(f *os.File, _ Enrollment, v Volume) error {
	var st unix.Stat_t
	if unix.Fstat(int(f.Fd()), &st) != nil || st.Dev != m.stat.Dev || st.Ino != m.stat.Ino || st.Size != v.Bytes {
		return ErrUncertain
	}
	return nil
}
func (m *hostFixtureMedia) ConfirmHostUnused(_ context.Context, _ Enrollment, commit func() error) error {
	if m.count() != 0 {
		return ErrHeld // includes actual borrowed and independently reopened files
	}
	return commit()
}

func sourceProviderFixture(t *testing.T) (*Provider, KubernetesSource, client.Client, *hostFixtureMedia, string) {
	t.Helper()
	source, cr, objects := sourceFixture(t)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	k := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	source.Reader = k
	path := filepath.Join(t.TempDir(), "selected")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(source.Config.Bytes); err != nil {
		t.Fatal(err)
	}
	m := &hostFixtureMedia{path: path}
	if err := unix.Fstat(int(f.Fd()), &m.stat); err != nil {
		t.Fatal(err)
	}
	f.Close()
	p, err := OpenDynamic(filepath.Join(t.TempDir(), "ledger"), source.Config, source, m)
	if err != nil {
		t.Fatal(err)
	}
	return p, source, k, m, cr.GetName()
}

func hostRequest(b HostBinding, a HostActivation, o HostOwner, op string) HostPacket {
	return HostPacket{Version: HostProtocol, Operation: op, Nonce: uuid.NewString(), Binding: b, Activation: a, Owner: o}
}

func exchangeHost(u *net.UnixConn, q HostPacket, input *os.File, want int) (HostPacket, *os.File, error) {
	var reply HostPacket
	_ = u.SetDeadline(time.Now().Add(5 * time.Second))
	if err := writeHostPacket(u, q, input); err != nil {
		return reply, nil, err
	}
	b, oob := make([]byte, 65536), make([]byte, unix.CmsgSpace(8*4))
	n, on, flags, _, err := u.ReadMsgUnix(b, oob)
	if err != nil || n == 0 {
		return reply, nil, errors.Join(io.EOF, err)
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil {
		return reply, nil, err
	}
	var files []*os.File
	for _, msg := range msgs {
		fds, err := unix.ParseUnixRights(&msg)
		if err != nil {
			tidyHostFiles(files)
			return reply, nil, err
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "borrowed-fixture"))
		}
	}
	if json.Unmarshal(b[:n], &reply) != nil || len(files) != want || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 ||
		reply.Version != q.Version || reply.Operation != q.Operation+"-ack" || reply.Nonce != q.Nonce ||
		reply.Owner != q.Owner || reply.Binding != q.Binding || reply.Activation != q.Activation {
		tidyHostFiles(files)
		return reply, nil, ErrUncertain
	}
	if want == 1 {
		return reply, files[0], nil
	}
	return reply, nil, nil
}
func tidyHostFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}

func startFixtureMember(t *testing.T, h *HostMember) func() *net.UnixConn {
	t.Helper()
	// A relative pathname keeps Linux sockaddr_un within its length limit.
	// Only test setup uses the current uid; NewHostMember requires real root.
	t.Chdir(t.TempDir())
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: "member.sock", Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	h.listener = l
	if err := os.Chmod("member.sock", 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return func() *net.UnixConn {
		t.Helper()
		u, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: "member.sock", Net: "unixpacket"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { u.Close() })
		return u
	}
}

func TestHostMemberRetainsRealOFDAndVerifiesTerminalUse(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("run this SCM_RIGHTS fixture as root to inspect real host namespaces; no block devices are used")
	}
	if err := hostKernelReady(); err != nil {
		t.Skipf("actual kernel lacks F_DUPFD_QUERY: %v", err)
	}
	p, source, _, media, composite := sourceProviderFixture(t)
	binding, err := CurrentHostBinding(source.Config.NodeName, "/var/lib", "/etc")
	if err != nil {
		t.Fatal(err)
	}
	h := &HostMember{db: p, media: media, binding: binding, uid: uint32(os.Geteuid())}
	if err := h.arm(); err != nil {
		t.Fatal(err)
	}
	p.host = h
	dial := startFixtureMember(t, h)
	ready := HostOwner{}
	ready.Allocation.Key.Provider = source.Config.PoolUUID
	if _, _, err := exchangeHost(dial(), hostRequest(binding, HostActivation{}, ready, "ready"), nil, 0); err != nil {
		t.Fatal("real member not ready", err)
	}
	r, err := p.ReserveInPool(t.Context(), source.Config.PoolUUID, "reserve", composite, source.Config.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	o := Owner{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID,
		ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation,
		NodeID: source.Config.NodeName, Epoch: 1}
	peer := sha256.Sum256([]byte("verified-client-leaf"))
	if _, err := p.Acquire(t.Context(), h.ForPeer(peer), "acquire", o, 0); err != nil {
		t.Fatal(err)
	}
	owner := hostOwner(o)
	a := HostActivation{ID: uuid.NewString(), Stage: "/var/lib/fixture/globalmount", Epoch: 1}
	a.Plan.Volume, a.Plan.Node, a.Plan.Incarnation, a.Plan.Backend = composite, o.NodeID, "composite-uid", "host"
	a.Plan.Grants[1] = HostGrant{Allocation: owner.Allocation, Pool: OrdinaryPool, Bytes: uint64(r.CapacityBytes)}
	a.Plan.Grants[0] = a.Plan.Grants[1]
	a.Plan.Grants[0].Allocation.Key.Provider = "origin-provider"
	a.Plan.Grants[0].Allocation.StableDeviceID = "origin-disk"
	a.Plan.Grants[0].Pool = "origin-pool"
	u := dial()
	retain := hostRequest(binding, a, owner, "retain-host")
	reply, f, err := exchangeHost(u, retain, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if reply.Opened.StableDeviceID != r.StableDeviceID || reply.Opened.CapacityBytes != uint64(r.CapacityBytes) {
		t.Fatal("did not return independently selected identity", reply.Opened)
	}
	bind := hostRequest(binding, a, owner, "bind-host-current")
	if _, _, err := exchangeHost(u, bind, f, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateOpen(t.Context(), h.ForPeer(peer), o, uint64(r.CapacityBytes), 253, 9); err != nil {
		t.Fatal("same-OFD current not registered", err)
	}
	otherPeer := sha256.Sum256([]byte("different-valid-client-leaf"))
	if err := p.ValidateOpen(t.Context(), h.ForPeer(otherPeer), o, uint64(r.CapacityBytes), 253, 9); !errors.Is(err, ErrStale) {
		t.Fatal("holder identity copied from another authenticated leaf", err)
	}
	// A separately reopened file has the same inode/device but NOT the OFD.
	reopened, err := os.OpenFile(media.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := exchangeHost(dial(), bind, reopened, 0); err == nil {
		t.Fatal("accepted separately reopened description")
	}
	reopened.Close()
	reconnect := dial()
	_, f2, err := exchangeHost(reconnect, retain, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	if err := sameHostOpen(f, f2); err != nil || media.opens.Load() != 1 {
		t.Fatal("reconnect replaced original", err, media.opens.Load())
	}
	if _, _, err := exchangeHost(reconnect, bind, f2, 0); err != nil {
		t.Fatal(err)
	}
	u.Close() // its delayed disconnect must not remove the successor binding
	if _, _, err := exchangeHost(reconnect, hostRequest(binding, a, owner, "check-host"), nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateOpen(t.Context(), h.ForPeer(peer), o, uint64(r.CapacityBytes), 253, 9); err != nil {
		t.Fatal(err)
	}
	if err := withLedger(p.directory, func(s *ledger, save func() error) error {
		s.Records[o.CompositeID].Claim.Deadline = time.Now().Add(-time.Minute).Unix()
		return save()
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := exchangeHost(dial(), retain, nil, 1); err == nil {
		t.Fatal("Held owner obtained a normal grant")
	}
	cleanupChannel := dial()
	cleanupRetain := hostRequest(binding, a, owner, "retain-host-cleanup")
	_, cleanupFD, err := exchangeHost(cleanupChannel, cleanupRetain, nil, 1)
	if err != nil {
		t.Fatal("same-owner retained cleanup OFD unavailable", err)
	}
	defer cleanupFD.Close()
	if err := sameHostOpen(f, cleanupFD); err != nil || media.opens.Load() != 1 {
		t.Fatal("cleanup reconstructed or replaced original", err)
	}
	if _, _, err := exchangeHost(cleanupChannel, hostRequest(binding, a, owner, "bind-host-cleanup"), cleanupFD, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := exchangeHost(cleanupChannel, hostRequest(binding, a, owner, "check-host-cleanup"), nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateOpen(t.Context(), h.ForPeer(peer), o, uint64(r.CapacityBytes), 253, 9); !errors.Is(err, ErrHeld) {
		t.Fatal("cleanup access revived v1 Validate", err)
	}
	if _, err := p.Renew(t.Context(), h.ForPeer(peer), "late-renew", o); !errors.Is(err, ErrHeld) {
		t.Fatal("cleanup revived renewal", err)
	}
	lostRegistry := &HostMember{db: p, media: media, binding: binding, uid: uint32(os.Geteuid())}
	if _, _, err := lostRegistry.handle(t.Context(), 99, cleanupRetain, nil); err == nil || media.opens.Load() != 1 {
		t.Fatal("restart reconstructed a lost original", err)
	}
	// Actual live borrowed files keep finish retained, independent of JSON.
	finish := hostRequest(binding, a, owner, "finish-host")
	if _, _, err := exchangeHost(dial(), finish, nil, 0); err == nil {
		t.Fatal("finish ignored real borrowed descriptors")
	}
	if _, _, err := exchangeHost(dial(), retain, nil, 1); err == nil {
		t.Fatal("terminal intent allowed reopen")
	}
	f.Close()
	f2.Close()
	cleanupFD.Close()
	cleanupChannel.Close()
	reconnect.Close()
	reply, _, err = exchangeHost(dial(), finish, nil, 0)
	if err != nil || reply.ReceiptKind != "native-host-teardown" || reply.Receipt.Owner != owner || reply.Receipt.ID == "" {
		t.Fatal("native host proof not committed", reply, err)
	}
	if err := p.ReleaseNode(t.Context(), h.ForPeer(peer), "release", o, "guest-clean"); !errors.Is(err, ErrHeld) {
		t.Fatal("guest alias accepted for native host", err)
	}
	if err := p.ReleaseNode(t.Context(), h.ForPeer(peer), "release", o, reply.Receipt.ID); err != nil {
		t.Fatal(err)
	}
	closed, _, err := exchangeHost(dial(), finish, nil, 0)
	if err != nil || closed.Receipt != reply.Receipt {
		t.Fatal("terminal replay did not use history", closed, err)
	}
	if err := p.BeforePhysical(r.Handle, ""); !errors.Is(err, ErrHeld) {
		t.Fatal("native receipt bypassed allocation closure", err)
	}
}

func TestHostKernelOFDProbeHasNoFallback(t *testing.T) {
	fd, err := unix.MemfdCreate("host-test", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), "original")
	defer f.Close()
	if err := hostKernelReady(); err != nil {
		if sameHostOpen(f, f) == nil {
			t.Fatal("unsupported query was treated as identity")
		}
		return
	}
	other, err := os.OpenFile("/proc/self/fd/"+strconv.Itoa(fd), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if sameHostOpen(f, other) == nil {
		t.Fatal("separate OFD of same inode accepted")
	}
}
