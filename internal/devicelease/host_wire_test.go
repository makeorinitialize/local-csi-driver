// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"encoding/json"
	"net"
	"os"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestHostMemberWireCompatibility(t *testing.T) {
	raw, err := os.ReadFile("testdata/host-member-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"pinned", "unknown-version", "caller-fingerprint", "extra-grant"} {
		t.Run(mode, func(t *testing.T) {
			var obj map[string]any
			if err := json.Unmarshal(raw, &obj); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unknown-version":
				obj["Version"] = "dmstack-host-ofd-v2"
			case "caller-fingerprint":
				obj["OpenedPeerSHA256"] = "caller-is-not-authority"
			case "extra-grant":
				plan := obj["Activation"].(map[string]any)["Plan"].(map[string]any)
				grants := plan["Grants"].([]any)
				plan["Grants"] = append(grants, grants[0])
			}
			packet, _ := json.Marshal(obj)
			pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			file := os.NewFile(uintptr(pair[0]), "parser")
			conn, err := net.FileConn(file)
			file.Close()
			if err != nil {
				unix.Close(pair[1])
				t.Fatal(err)
			}
			defer conn.Close()
			defer unix.Close(pair[1])
			if _, err := unix.Write(pair[1], packet); err != nil {
				t.Fatal(err)
			}
			q, f, err := readHostPacket(conn.(*net.UnixConn))
			if f != nil {
				f.Close()
				t.Fatal("unexpected input descriptor")
			}
			if mode != "pinned" {
				if err == nil {
					t.Fatal("malformed member contract accepted")
				}
				return
			}
			if err != nil || !q.Activation.valid(q.Binding, q.Owner) {
				t.Fatal(q, err)
			}
			back, _ := json.Marshal(q)
			var roundtrip map[string]any
			_ = json.Unmarshal(back, &roundtrip)
			if !reflect.DeepEqual(obj, roundtrip) {
				t.Fatal("provider field contract diverged from pinned client packet")
			}
		})
	}
}
