// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"local-csi-driver/internal/csi/core/lvm"
)

func configureCanaryBootstrap(name, group, journal, nodeSocket, controllerSocket string, webhook bool) error {
	if name == lvm.OfficialDriverName {
		if os.Getenv("ACS_DYNAMIC_BLOCK") == "1" {
			return errors.New("online enrollment requires a separately registered canary CSI driver, not the managed addon")
		}
		return lvm.ConfigureCanary(name, group)
	}
	if os.Getenv("ACS_DYNAMIC_BLOCK") != "1" || os.Getenv("ACS_RAW_CACHE") != "1" || webhook {
		return errors.New("separate canary requires explicit online opt-ins and no managed-addon webhook")
	}
	root := filepath.Dir(journal)
	if !filepath.IsAbs(journal) || filepath.Clean(journal) != journal ||
		root == "/" || root == filepath.Dir(defaultOrdinaryDirectory) {
		return errors.New("canary journal needs its own absolute plugin directory, not /csi-node")
	}
	if nodeSocket == controllerSocket {
		return errors.New("canary node/controller sockets must be distinct")
	}
	for _, address := range []string{nodeSocket, controllerSocket} {
		path, ok := strings.CutPrefix(address, "unix://")
		if !ok || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != root {
			return errors.New("canary CSI sockets must be explicit paths in the isolated journal parent")
		}
	}
	return lvm.ConfigureCanary(name, group)
}
