// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"local-csi-driver/internal/devicelease"
)

func hostMember(node string, p *devicelease.Provider, rpcPort int) (*devicelease.HostMember, error) {
	mode := os.Getenv("ACS_HOST_CUSTODY")
	if mode == "" {
		if p != nil {
			configured, err := p.HostConfigured()
			if err != nil || configured {
				return nil, errors.Join(devicelease.ErrUncertain, err)
			}
		}
		return nil, nil
	}
	if mode != "1" || os.Getenv("ACS_DYNAMIC_BLOCK") != "1" || p == nil || rpcPort == 0 {
		return nil, errors.New("host custody requires online single-canary mode and authenticated v1 listener")
	}
	path := filepath.Join(enrollmentConfigDirectory, "host.json")
	if err := protectedFile(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg struct{ Socket, StageRoot, PodsRoot string }
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&cfg); err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, devicelease.ErrUncertain
	}
	binding, err := devicelease.CurrentHostBinding(node, cfg.StageRoot, cfg.PodsRoot)
	if err != nil {
		return nil, err
	}
	return devicelease.NewHostMember(p, cfg.Socket, binding)
}
