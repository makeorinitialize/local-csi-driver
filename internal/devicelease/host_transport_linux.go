// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	strictjson "sigs.k8s.io/json"
)

func readHostPacket(u *net.UnixConn) (HostPacket, *os.File, error) {
	var q HostPacket
	b, oob := make([]byte, 65536), make([]byte, unix.CmsgSpace(8*4))
	n, on, flags, _, err := u.ReadMsgUnix(b, oob)
	if err != nil {
		return q, nil, err
	}
	messages, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil {
		return q, nil, err
	}
	var files []*os.File
	keep := false
	defer func() {
		if !keep {
			for _, f := range files {
				_ = f.Close()
			}
		}
	}()
	bad := false
	for _, m := range messages {
		fds, err := unix.ParseUnixRights(&m)
		if err != nil {
			bad = true
			continue
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "acs-host-input"))
		}
	}
	problems, parseErr := strictjson.UnmarshalStrict(b[:n], &q)
	if bad || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || parseErr != nil || len(problems) != 0 || q.Version != HostProtocol {
		return HostPacket{}, nil, ErrUncertain
	}
	want := 0
	if q.Operation == "bind-host-current" || q.Operation == "bind-host-cleanup" {
		want = 1
	}
	if len(files) != want {
		return HostPacket{}, nil, ErrUncertain
	}
	// encoding/json silently truncates extra elements of fixed arrays.
	if q.Operation != "ready" {
		var shape struct {
			Activation struct {
				Plan struct{ Grants []json.RawMessage }
			}
		}
		if json.Unmarshal(b[:n], &shape) != nil || len(shape.Activation.Plan.Grants) != 2 {
			return HostPacket{}, nil, ErrUncertain
		}
	}
	keep = true
	if want == 1 {
		return q, files[0], nil
	}
	return q, nil, nil
}

func writeHostPacket(u *net.UnixConn, q HostPacket, f *os.File) error {
	b, err := json.Marshal(q)
	if err != nil || len(b) > 65536 {
		return errors.Join(ErrUncertain, err)
	}
	var oob []byte
	if f != nil {
		oob = unix.UnixRights(int(f.Fd()))
	}
	n, on, err := u.WriteMsgUnix(b, oob, nil)
	runtime.KeepAlive(f)
	if err != nil || n != len(b) || on != len(oob) {
		return errors.Join(ErrUncertain, err)
	}
	return nil
}

func (h *HostMember) serve(ctx context.Context, u *net.UnixConn) {
	defer u.Close()
	raw, err := u.SyscallConn()
	if err != nil {
		return
	}
	var peer *unix.Ucred
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		peer, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil || socketErr != nil || peer == nil || peer.Uid != h.uid {
		return
	}
	init, err := os.Stat("/proc/1/ns/pid")
	if err != nil {
		return
	}
	client, err := os.Stat(fmt.Sprintf("/proc/%d/ns/pid", peer.Pid))
	if err != nil || !os.SameFile(init, client) {
		return
	}
	token := h.tokens.Add(1)
	if token == 0 {
		return
	}
	defer h.disconnect(token)
	stop := context.AfterFunc(ctx, func() { _ = u.Close() })
	defer stop()
	for {
		if err := u.SetDeadline(time.Now().Add(time.Minute)); err != nil {
			return
		}
		q, f, err := readHostPacket(u)
		if err != nil {
			return
		}
		op, cancel := context.WithTimeout(ctx, 10*time.Second)
		reply, output, err := h.handle(op, token, q, f)
		cancel()
		if f != nil {
			_ = f.Close()
		}
		if err == nil {
			err = u.SetWriteDeadline(time.Now().Add(5 * time.Second))
		}
		if err == nil {
			err = writeHostPacket(u, reply, output)
		}
		if output != nil {
			_ = output.Close()
		}
		if err != nil {
			return // never synthesize an acknowledgement for uncertain state
		}
	}
}

func (h *HostMember) Start(ctx context.Context) error {
	if h.listener == nil {
		return ErrUncertain
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	slots := make(chan struct{}, 8)
	stop := context.AfterFunc(ctx, func() { _ = h.listener.Close() })
	defer stop()
	defer func() {
		h.stopped.Store(true)
		cancel()
		_ = h.listener.Close()
		wg.Wait()
		h.mu.Lock()
		defer h.mu.Unlock()
		if e := h.entry; e != nil {
			if e.current != nil {
				_ = e.current.Close()
			}
			if e.original != nil {
				_ = e.original.Close()
			}
			h.entry = nil
		}
	}()
	for {
		u, err := h.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
			wg.Add(1)
			go func() { defer wg.Done(); defer func() { <-slots }(); h.serve(ctx, u) }()
		default:
			_ = u.Close()
		}
	}
}
