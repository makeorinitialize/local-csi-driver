// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBackingDriverDefaultsPreserveOldFingerprint(t *testing.T) {
	source, cr, objects := sourceFixture(t)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	source.Reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	old, err := source.Resolve(t.Context(), cr.GetName(), source.Config.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	before := cr.GetAnnotations()[BackingJournalKey]
	j, err := parseBacking(cr)
	if err != nil {
		t.Fatal(err)
	}
	j.Config.OriginDriver, j.Config.CacheDriver = OfficialOriginDriver, OfficialCacheDriver
	data, _ := json.Marshal(j)
	annotations := cr.GetAnnotations()
	annotations[BackingJournalKey] = string(data)
	cr.SetAnnotations(annotations)
	source.Reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	explicit := source.Config
	explicit.OriginDriver, explicit.CacheDriver = OfficialOriginDriver, OfficialCacheDriver
	if !source.Config.same(explicit) || source.Config.CacheDriver != "" || source.Config.OriginDriver != "" {
		t.Fatal("default equivalence changed stored configuration")
	}
	source.Config = explicit
	now, err := source.Resolve(t.Context(), cr.GetName(), source.Config.Bytes)
	if err != nil || now != old || before == string(data) || cr.GetAnnotations()[BackingJournalKey] != string(data) {
		t.Fatal("explicit defaults changed identity or rewrote annotation", now, old, err)
	}
}

func TestBackingCustomDriversAreExactPins(t *testing.T) {
	for _, suffix := range []string{"dmstack.infinitydb.io", "canary.example"} {
		for _, bad := range []string{"", "official-pv", "foreign-class", "cross-role", "uppercase"} {
			t.Run(suffix+"/"+bad, func(t *testing.T) {
				source, cr, objects := sourceFixture(t)
				j, err := parseBacking(cr)
				if err != nil {
					t.Fatal(err)
				}
				j.Config.OriginDriver, j.Config.CacheDriver = "disk."+suffix, "localdisk."+suffix
				source.Config.OriginDriver, source.Config.CacheDriver = j.Config.OriginDriver, j.Config.CacheDriver
				objects[3].(*storagev1.StorageClass).Provisioner = j.Config.OriginDriver
				objects[5].(*corev1.PersistentVolume).Spec.CSI.Driver = j.Config.OriginDriver
				objects[6].(*storagev1.StorageClass).Provisioner = j.Config.CacheDriver
				objects[8].(*corev1.PersistentVolume).Spec.CSI.Driver = j.Config.CacheDriver
				switch bad {
				case "official-pv":
					objects[8].(*corev1.PersistentVolume).Spec.CSI.Driver = OfficialCacheDriver
				case "foreign-class":
					objects[6].(*storagev1.StorageClass).Provisioner = "foreign.example"
				case "cross-role":
					j.Config.CacheDriver, source.Config.CacheDriver = OfficialOriginDriver, OfficialOriginDriver
					objects[6].(*storagev1.StorageClass).Provisioner = OfficialOriginDriver
					objects[8].(*corev1.PersistentVolume).Spec.CSI.Driver = OfficialOriginDriver
				case "uppercase":
					j.Config.CacheDriver = "LOCALDISK." + suffix
				}
				raw, _ := json.Marshal(j)
				annotations := cr.GetAnnotations()
				annotations[BackingJournalKey] = string(raw)
				cr.SetAnnotations(annotations)
				scheme := runtime.NewScheme()
				_ = corev1.AddToScheme(scheme)
				_ = storagev1.AddToScheme(scheme)
				source.Reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
				_, err = source.Resolve(t.Context(), cr.GetName(), source.Config.Bytes)
				if (err != nil) != (bad != "") {
					t.Fatal("driver alias/default substituted for exact identity", err)
				}
			})
		}
	}
}
