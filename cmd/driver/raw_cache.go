// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"

	"local-csi-driver/internal/devicelease"
	"local-csi-driver/internal/devicelease/lvbackend"
	"local-csi-driver/internal/devicelease/rpc"
	lvmMgr "local-csi-driver/internal/pkg/lvm"
)

const ledgerDirectory = "/var/lib/acs-dmstack-raw"

// The plugin directory is a persistent host mount even with rawCache disabled.
const ordinaryDirectory = "/csi-node/disposable-block-hold"

func rawCacheListener(port int, node string, manager lvmMgr.Manager, ordinary *devicelease.Provider) (*rpc.Listener, error) {
	if os.Getenv("ACS_RAW_CACHE") != "1" || port < 1 || port > 65535 {
		return nil, errors.New("raw-cache RPC requires explicit opt-in and a valid port")
	}
	if ordinary != nil {
		return rpc.New(":"+strconv.Itoa(port), "/etc/acs-dmstack-provider", node, ordinary)
	}
	p, err := devicelease.Open(ledgerDirectory, lvbackend.Backend{Manager: manager})
	if err != nil {
		return nil, err
	}
	return rpc.New(":"+strconv.Itoa(port), "/etc/acs-dmstack-provider", node, p)
}

// Private operator command, independent of the optional mTLS listener.
func rawCache(args []string) error {
	if os.Getenv("ACS_RAW_CACHE") != "1" || os.Geteuid() != 0 {
		return errors.New("raw-cache requires opt-in and local operator privileges")
	}
	if len(args) == 1 && args[0] == "enroll-block" {
		d := json.NewDecoder(os.Stdin)
		d.DisallowUnknownFields()
		var e devicelease.Enrollment
		if err := d.Decode(&e); err != nil {
			return err
		}
		if d.Decode(new(any)) != io.EOF || e.NodeID == "" || e.NodeID != os.Getenv("NODE_NAME") {
			return errors.New("invalid or wrong-node enrollment")
		}
		return devicelease.InitializeOrdinary(context.Background(), ordinaryDirectory, e, lvbackend.Backend{Manager: lvmMgr.NewClient()})
	}
	p, err := devicelease.Open(ledgerDirectory, lvbackend.Backend{Manager: lvmMgr.NewClient()})
	if err != nil {
		return err
	}
	switch {
	case len(args) == 1 && args[0] == "init":
		return p.Initialize(context.Background())
	case len(args) == 3 && args[0] == "inspect":
		r, err := p.Lookup(context.Background(), args[1], args[2])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case len(args) == 4 && args[0] == "reserve":
		n, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
		r, err := p.Reserve(context.Background(), args[1], args[2], n)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	default:
		return errors.New("usage: raw-cache init | reserve REQUEST_ID COMPOSITE_ID BYTES | inspect POOL_UUID LEASE_ID; node access and release are not supported")
	}
}
