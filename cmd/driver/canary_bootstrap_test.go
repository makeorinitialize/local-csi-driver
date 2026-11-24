// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"testing"

	"local-csi-driver/internal/csi/core/lvm"
)

func TestSeparateCanaryBootstrapRequiresIsolatedConfiguration(t *testing.T) {
	t.Cleanup(func() { _ = lvm.ConfigureCanary(lvm.OfficialDriverName, "") })
	for _, tc := range []struct {
		name, group, config, node, controller string
	}{
		{"localdisk.compositedisk.infinitydb.io", "canary-vg", "", "unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock"},
		{"localdisk.compositedisk.infinitydb.io", "containerstorage", "/csi-canary/hold", "unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock"},
		{"LOCALDISK.compositedisk.infinitydb.io", "canary-vg", "/csi-canary/hold", "unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock"},
		{"localdisk.compositedisk.infinitydb.io", "canary-vg", "/csi-node/other-hold", "unix:///csi-node/node.sock", "unix:///csi-node/control.sock"},
		{"localdisk.compositedisk.infinitydb.io", "canary-vg", "/csi-canary/hold", "unix:///csi-node/csi.sock", "unix:///csi-canary/control.sock"},
	} {
		if err := configureCanaryBootstrap(tc.name, tc.group, tc.config, tc.node, tc.controller, false); err == nil {
			t.Fatal("unsafe or aliased canary bootstrap", tc)
		}
	}
	if err := configureCanaryBootstrap("localdisk.compositedisk.infinitydb.io", "canary-vg", "/csi-canary/hold",
		"unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock", false); err != nil {
		t.Fatal(err)
	}
}
