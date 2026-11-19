// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"local-csi-driver/internal/devicelease"
	"local-csi-driver/internal/devicelease/lvbackend"
)

type custodian string

type ordinaryCustodian struct{ custodian }

func (c ordinaryCustodian) Closed(o devicelease.Owner, id string) error {
	if err := c.confirm("guest-clean", o, id, nil); err != nil {
		return err
	}
	return c.confirm("host-unplugged", o, id, nil)
}

type challenge struct {
	Version                   int
	Nonce, Operation, Receipt string
	Owner                     devicelease.Owner
}

func (c custodian) Enrolled(o devicelease.Owner) error { return c.confirm("enrolled", o, "", nil) }
func (c custodian) Closed(o devicelease.Owner, id string) error {
	return c.confirm("closed", o, id, nil)
}
func (c custodian) Opened(o devicelease.Owner, v devicelease.Volume) error {
	return c.confirm("opened", o, "", &v)
}

// The socket path is fixed in the separately administered read-only identity
// mount. A client cannot supply an attestation, descriptor or custodian path.
func (c custodian) confirm(op string, o devicelease.Owner, id string, v *devicelease.Volume) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	q := challenge{1, hex.EncodeToString(nonce[:]), op, id, o}
	data, err := json.Marshal(q)
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("unixpacket", string(c), 3*time.Second)
	if err != nil {
		return devicelease.ErrUncertain
	}
	defer conn.Close()
	u := conn.(*net.UnixConn)
	raw, err := u.SyscallConn()
	if err != nil {
		return err
	}
	var peer *unix.Ucred
	if err := raw.Control(func(fd uintptr) { peer, _ = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil || peer == nil || peer.Uid != 0 {
		return devicelease.ErrUncertain
	}
	if err := u.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	if _, err := u.Write(data); err != nil {
		return err
	}
	buf, anc := make([]byte, 128<<10), make([]byte, unix.CmsgSpace(4*16))
	n, an, flags, _, err := u.ReadMsgUnix(buf, anc)
	if err != nil {
		return err
	}
	msgs, err := unix.ParseSocketControlMessage(anc[:an])
	if err != nil {
		return err
	}
	var fds []int
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	for _, m := range msgs {
		rights, err := unix.ParseUnixRights(&m)
		if err != nil {
			return err
		}
		fds = append(fds, rights...)
	}
	var reply struct {
		Challenge challenge
		Confirmed bool
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || json.Unmarshal(buf[:n], &reply) != nil || reply.Challenge != q {
		return devicelease.ErrUncertain
	}
	if !reply.Confirmed {
		return devicelease.ErrHeld
	}
	if v == nil && len(fds) == 0 {
		return nil
	}
	if v == nil || len(fds) != 1 {
		return devicelease.ErrUncertain
	}
	return lvbackend.VerifyDescriptor(fds[0], o.PoolUUID, o.StableDeviceID, *v)
}
