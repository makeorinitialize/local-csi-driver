// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import "testing"

func TestOrdinaryTarget(t *testing.T) {
	for _, tc := range []struct {
		vg, lv string
		ok     bool
	}{
		{Group, "pvc-old", false},
		{"mapper", "acs--dmstack--raw--v1-raw--abc", false},
		{"other", "../" + Group + "/raw-abc", false},
		{"/dev/" + Group, "raw-abc", false},
		{"@acs.dmstack.raw.pool.v1", "raw-abc", false},
		{"ordinary.custom-vg", "raw-abc", true},
		{Group + "-legacy", "pvc-123", true},
	} {
		if got := OrdinaryTarget(tc.vg, tc.lv); got != tc.ok {
			t.Errorf("%s#%s: ordinary=%t, want %t", tc.vg, tc.lv, got, tc.ok)
		}
	}
}
