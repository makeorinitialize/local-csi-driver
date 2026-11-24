// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"errors"
	"path/filepath"
	"strings"

	"local-csi-driver/internal/csi/core/lvm"
)

func configureCanaryBootstrap(name, group, configPath, nodeSocket, controllerSocket string, webhook bool) error {
	if name == lvm.OfficialDriverName {
		return lvm.ConfigureCanary(name, group)
	}
	if configPath == "" || webhook {
		return errors.New("separate canary requires explicit device-use configuration and no managed-addon webhook")
	}
	nodePath, ok := strings.CutPrefix(nodeSocket, "unix://")
	if !ok {
		return errors.New("canary requires an explicit node socket")
	}
	root := filepath.Dir(nodePath)
	if root == "/" || root == "/csi-node" {
		return errors.New("canary needs its own plugin directory, not /csi-node")
	}
	if nodeSocket == controllerSocket {
		return errors.New("canary node/controller sockets must be distinct")
	}
	for _, address := range []string{nodeSocket, controllerSocket} {
		path, ok := strings.CutPrefix(address, "unix://")
		if !ok || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != root {
			return errors.New("canary CSI sockets must be explicit paths in the isolated plugin directory")
		}
	}
	return lvm.ConfigureCanary(name, group)
}
