// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import "k8s.io/apimachinery/pkg/util/validation"

const (
	OfficialOriginDriver = "disk.csi.azure.com"
	OfficialCacheDriver  = "localdisk.csi.acstor.io"
)

func originDriver(name string) string {
	if name == "" {
		return OfficialOriginDriver
	}
	return name
}
func cacheDriver(name string) string {
	if name == "" {
		return OfficialCacheDriver
	}
	return name
}
func validDrivers(origin, cache string) bool {
	origin, cache = originDriver(origin), cacheDriver(cache)
	return len(origin) <= 63 && len(cache) <= 63 &&
		len(validation.IsDNS1123Subdomain(origin)) == 0 && len(validation.IsDNS1123Subdomain(cache)) == 0 &&
		origin != cache && origin != OfficialCacheDriver && cache != OfficialOriginDriver
}

func (c DynamicConfig) OriginDriverName() string { return originDriver(c.OriginDriver) }
func (c DynamicConfig) CacheDriverName() string  { return cacheDriver(c.CacheDriver) }

func (c DynamicConfig) same(other DynamicConfig) bool {
	c.OriginDriver, c.CacheDriver = c.OriginDriverName(), c.CacheDriverName()
	other.OriginDriver, other.CacheDriver = other.OriginDriverName(), other.CacheDriverName()
	return c == other
}

// Separate CSI registration alone does not exclude the managed addon's LVM
// orphan/reaper scans. An alias domain must not carry the official local-csi tag.
func OrdinaryVGTag(driver string) string {
	if cacheDriver(driver) == OfficialCacheDriver {
		return "local-csi"
	}
	return "dmstack.csi." + driver
}
