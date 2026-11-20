// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package main

import (
	"testing"

	"local-csi-driver/internal/csi/core/lvm"
)

func TestSeparateCanaryBootstrapRefusesManagedIdentity(t *testing.T) {
	t.Setenv("ACS_DYNAMIC_BLOCK", "1")
	t.Setenv("ACS_RAW_CACHE", "1")
	t.Cleanup(func() { _ = lvm.ConfigureCanary(lvm.OfficialDriverName, "") })
	for _, tc := range []struct {
		name, group, journal, node, controller string
	}{
		{lvm.OfficialDriverName, "", "/csi-node/disposable-block-hold", "unix:///csi-node/node.sock", "unix:///csi-node/control.sock"},
		{"localdisk.dmstack.infinitydb.io", "containerstorage", "/csi-canary/hold", "unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock"},
		{"LOCALDISK.dmstack.infinitydb.io", "canary-vg", "/csi-canary/hold", "unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock"},
		{"localdisk.dmstack.infinitydb.io", "canary-vg", "/csi-node/other-hold", "unix:///csi-node/node.sock", "unix:///csi-node/control.sock"},
		{"localdisk.dmstack.infinitydb.io", "canary-vg", "/csi-canary/hold", "unix:///csi-node/csi.sock", "unix:///csi-canary/control.sock"},
	} {
		if err := configureCanaryBootstrap(tc.name, tc.group, tc.journal, tc.node, tc.controller, false); err == nil {
			t.Fatal("unsafe or aliased canary bootstrap", tc)
		}
	}
	if err := configureCanaryBootstrap("localdisk.dmstack.infinitydb.io", "canary-vg", "/csi-canary/hold",
		"unix:///csi-canary/node.sock", "unix:///csi-canary/control.sock", false); err != nil {
		t.Fatal(err)
	}
}
