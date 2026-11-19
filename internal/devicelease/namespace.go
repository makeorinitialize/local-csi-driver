// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Package devicelease manages node-local raw cache reservations, not CSI PVs.
package devicelease

import (
	"regexp"
	"strings"
)

const (
	Group    = "acs-dmstack-raw-v1"
	PoolTag  = "acs.dmstack.raw.pool.v1"
	ownerTag = "acs.dmstack.raw.lv.v1"
)

var component = regexp.MustCompile(`^[a-zA-Z0-9_+.][a-zA-Z0-9_+.-]*$`)

func validName(s string) bool {
	return s != "." && s != ".." && component.MatchString(s)
}

// OrdinaryTarget rejects path/option/tag aliases as well as the reserved VG.
// The mapper spelling matters for consumers that reconstruct a /dev path.
func OrdinaryTarget(vg, lv string) bool {
	return validName(vg) && validName(lv) && vg != Group &&
		!(vg == "mapper" && strings.HasPrefix(lv, strings.ReplaceAll(Group, "-", "--")+"-"))
}
