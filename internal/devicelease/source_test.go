// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func sourceFixture(t *testing.T) (KubernetesSource, *unstructured.Unstructured, []client.Object) {
	t.Helper()
	raw, err := os.ReadFile("testdata/external-source-pvcs-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	v := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.infinitydb.io/v1alpha1", "kind": "CompositeVolume",
		"spec": map[string]any{"capacity": int64(8192), "cacheBytes": int64(4096),
			"cacheParameters": map[string]any{"provider": "vg-uuid", "poolID": OrdinaryPool, "requestID": "reserve"},
			"topology":        map[string]any{sourcePrefix + "node": "canary-node"}},
	}}
	v.SetName("dmstack-" + strings.Repeat("a", 52))
	v.SetUID("composite-uid")
	v.SetResourceVersion("1")
	v.SetFinalizers([]string{sourcePrefix + "allocations"})
	v.SetAnnotations(map[string]string{ExternalSourceKey: string(raw), sourcePrefix + "selected-node": "canary-node"})
	j, err := parseExternal(v)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DynamicConfig{NodeName: "canary-node", NodeUID: "node-uid", Namespace: "canary-backing",
		NamespaceUID: "namespace-uid", CacheClass: "cache", CacheClassUID: "cache-class-uid",
		VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	objects := []client.Object{v,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cfg.NodeName, UID: types.UID(cfg.NodeUID)}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, UID: types.UID(cfg.NamespaceUID)}}}
	for _, pin := range []externalClaim{j.Origin, j.Cache} {
		mode := corev1.PersistentVolumeBlock
		binding := storagev1.VolumeBindingWaitForFirstConsumer
		q := *resource.NewQuantity(pin.Bytes, resource.BinarySI)
		objects = append(objects,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: pin.ClassName, UID: types.UID(pin.ClassUID)},
				Provisioner: pin.Driver, VolumeBindingMode: &binding},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pin.Name, Namespace: cfg.Namespace, UID: types.UID(pin.UID)},
				Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &pin.ClassName, VolumeName: pin.PVName, VolumeMode: &mode,
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: q}}},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: q}}},
			&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pin.PVName, UID: types.UID(pin.PVUID)},
				Spec: corev1.PersistentVolumeSpec{StorageClassName: pin.ClassName, VolumeMode: &mode,
					AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Capacity:               corev1.ResourceList{corev1.ResourceStorage: q},
					ClaimRef:               &corev1.ObjectReference{Name: pin.Name, Namespace: cfg.Namespace, UID: types.UID(pin.UID)},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: pin.Driver, VolumeHandle: pin.Handle}},
					NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{cfg.NodeName}}}},
					}}}},
				Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
		)
	}
	fs := corev1.PersistentVolumeFilesystem
	objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: j.Outer.Name, Namespace: cfg.Namespace, UID: types.UID(j.Outer.UID),
		Annotations: map[string]string{sourcePrefix + "origin-pvc": j.Origin.Name, sourcePrefix + "cache-pvc": j.Cache.Name}},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeMode: &fs, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: *resource.NewQuantity(j.Origin.Bytes, resource.BinarySI)}}}})
	return KubernetesSource{Config: cfg}, v, objects
}

func TestAuthoritativeExternalSourceCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*unstructured.Unstructured, []client.Object)
	}{
		{"external-names-no-ownership-markers", nil},
		{"node-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[1].SetUID("replacement") }},
		{"namespace-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[2].SetUID("replacement") }},
		{"class-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[6].SetUID("replacement") }},
		{"pvc-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[7].SetUID("replacement") }},
		{"pv-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[8].SetUID("replacement") }},
		{"origin-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[5].SetUID("replacement") }},
		{"outer-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[9].SetUID("replacement") }},
		{"outer-reference-drift", func(_ *unstructured.Unstructured, o []client.Object) {
			o[9].(*corev1.PersistentVolumeClaim).Annotations[sourcePrefix+"cache-pvc"] = "other"
		}},
		{"handle", func(_ *unstructured.Unstructured, o []client.Object) {
			o[8].(*corev1.PersistentVolume).Spec.CSI.VolumeHandle += "-changed"
		}},
		{"node-binding", func(_ *unstructured.Unstructured, o []client.Object) {
			o[8].(*corev1.PersistentVolume).Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchFields[0].Values = []string{"other"}
		}},
		{"deleting", func(v *unstructured.Unstructured, _ []client.Object) {
			now := metav1.Now()
			v.SetDeletionTimestamp(&now)
		}},
		{"cr-incarnation", func(v *unstructured.Unstructured, _ []client.Object) { v.SetUID("replacement") }},
		{"unknown-version", func(v *unstructured.Unstructured, _ []client.Object) {
			a := v.GetAnnotations()
			a[ExternalSourceKey] = strings.Replace(a[ExternalSourceKey], `"version": 1`, `"version": 2`, 1)
			v.SetAnnotations(a)
		}},
		{"legacy-owned-object", func(v *unstructured.Unstructured, _ []client.Object) {
			a := v.GetAnnotations()
			a[sourcePrefix+"backing-pvcs-v1"] = "{}"
			v.SetAnnotations(a)
		}},
		{"incomplete-owner", func(v *unstructured.Unstructured, _ []client.Object) {
			j, _ := parseExternal(v)
			j.Owner = ""
			b, _ := json.Marshal(j)
			a := v.GetAnnotations()
			a[ExternalSourceKey] = string(b)
			v.SetAnnotations(a)
		}},
		{"self-source", func(v *unstructured.Unstructured, _ []client.Object) {
			j, _ := parseExternal(v)
			j.Cache.Name = j.Outer.Name
			b, _ := json.Marshal(j)
			a := v.GetAnnotations()
			a[ExternalSourceKey] = string(b)
			v.SetAnnotations(a)
		}},
		{"namespace-expansion", func(v *unstructured.Unstructured, _ []client.Object) {
			j, _ := parseExternal(v)
			j.Cache.Name = "other/existing-cache"
			b, _ := json.Marshal(j)
			a := v.GetAnnotations()
			a[ExternalSourceKey] = string(b)
			v.SetAnnotations(a)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k, v, objects := sourceFixture(t)
			if tc.mutate != nil {
				tc.mutate(v, objects)
			}
			s := runtime.NewScheme()
			_ = corev1.AddToScheme(s)
			_ = storagev1.AddToScheme(s)
			k.Reader = fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()
			got, err := k.Resolve(t.Context(), v.GetName(), k.Config.Bytes)
			if tc.mutate != nil {
				if err == nil {
					t.Fatal("invalid source admitted", got)
				}
				return
			}
			if err != nil || got.Snapshot == "" || got.CompositeUID != "composite-uid" {
				t.Fatal(got, err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: k.Config.Namespace},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "cache", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: objects[7].GetName()}}}}}}
			if err := k.Reader.(client.Client).Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			if _, err := k.Resolve(t.Context(), v.GetName(), k.Config.Bytes); err == nil {
				t.Fatal("source consumer admitted")
			}
		})
	}
}

func TestRetainedExternalSourceSurvivesAPIDeletion(t *testing.T) {
	p, source, k, media, composite := sourceProviderFixture(t)
	r, err := p.ReserveInPool(t.Context(), source.Config.PoolUUID, "reserve", composite, source.Config.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	o := Owner{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID,
		ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation,
		NodeID: source.Config.NodeName, Epoch: 1}
	authority := proof{owner: o}
	if _, err := p.Acquire(t.Context(), authority, "acquire", o, 0); err != nil {
		t.Fatal(err)
	}
	// No finalizers or deletion calls are added to externally owned sources.
	var claims corev1.PersistentVolumeClaimList
	if err := k.List(t.Context(), &claims, client.InNamespace(source.Config.Namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range claims.Items {
		if len(claims.Items[i].Finalizers) != 0 {
			t.Fatal("provider took source metadata ownership")
		}
		if err := k.Delete(t.Context(), &claims.Items[i]); err != nil {
			t.Fatal(err)
		}
	}
	cr := &unstructured.Unstructured{}
	cr.SetAPIVersion("storage.infinitydb.io/v1alpha1")
	cr.SetKind("CompositeVolume")
	if err := k.Get(t.Context(), client.ObjectKey{Name: composite}, cr); err != nil {
		t.Fatal(err)
	}
	cr.SetFinalizers(nil)
	if err := k.Update(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	if err := k.Delete(t.Context(), cr); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenDynamic(p.directory, source.Config, source, media)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Lookup(t.Context(), r.PoolUUID, r.LeaseID); err != nil {
		t.Fatal("API deletion erased an existing hold", err)
	}
	if _, err := restarted.Renew(t.Context(), authority, "renew", o); err == nil {
		t.Fatal("fresh data admission without sources")
	}
	if _, err := restarted.Acquire(t.Context(), authority, "acquire", o, 0); err == nil {
		t.Fatal("Acquire replay admitted new data use without live source pins")
	}
	if err := restarted.ValidateOpen(t.Context(), proof{owner: o, open: true}, o, uint64(r.CapacityBytes), 253, 9); err == nil {
		t.Fatal("Validate admitted data use without live source pins")
	}
	media.wrongIdentity.Store(true)
	if _, err := restarted.Lookup(t.Context(), r.PoolUUID, r.LeaseID); !errors.Is(err, ErrUncertain) {
		t.Fatal("retained metadata substituted for actual physical identity", err)
	}
	if err := restarted.ReleaseNode(t.Context(), authority, "release", o, "receipt"); !errors.Is(err, ErrUncertain) {
		t.Fatal("cleanup accepted a different LV", err)
	}
	media.wrongIdentity.Store(false)
	stale := o
	stale.Epoch++
	if err := restarted.ReleaseNode(t.Context(), authority, "stale", stale, "receipt"); !errors.Is(err, ErrStale) {
		t.Fatal("stale owner used retained source", err)
	}
	if err := restarted.ReleaseNode(t.Context(), authority, "release", o, "unverified"); !errors.Is(err, ErrHeld) {
		t.Fatal("API absence substituted for cleanup proof", err)
	}
	if err := restarted.BeforePhysical(r.Handle, ""); !errors.Is(err, ErrHeld) {
		t.Fatal("API absence released native exclusion", err)
	}
	if err := restarted.ReleaseNode(t.Context(), authority, "release", o, "receipt"); err != nil {
		t.Fatal("same-owner positive cleanup depended on API existence", err)
	}
	if _, err := restarted.Acquire(t.Context(), proof{owner: stale}, "higher-epoch", stale, o.Epoch); !errors.Is(err, ErrStale) {
		t.Fatal("single-use limitation silently became epoch reuse", err)
	}
	if err := restarted.BeforePhysical(r.Handle, ""); !errors.Is(err, ErrHeld) {
		t.Fatal("node-use release substituted for allocation closure", err)
	}
	if err := restarted.ReleaseAllocation(t.Context(), "close", Allocation{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID,
		LeaseID: r.LeaseID, ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation}, o.Epoch); err != nil {
		t.Fatal(err)
	}
	finish, err := restarted.BeginProvision(r.Handle)
	if err != nil {
		t.Fatal("closed composite retained a hold over ordinary source lifecycle", err)
	}
	if err := finish(true); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ReserveInPool(t.Context(), r.PoolUUID, "reserve", composite, r.CapacityBytes); !errors.Is(err, ErrHeld) {
		t.Fatal("native source release rearmed provider slot", err)
	}

}

func TestExternalSourcesRemainAfterAllocationClosure(t *testing.T) {
	p, source, k, _, composite := sourceProviderFixture(t)
	r, err := p.ReserveInPool(t.Context(), source.Config.PoolUUID, "reserve", composite, source.Config.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ReleaseAllocation(t.Context(), "close-unused", Allocation{
		PoolUUID: r.PoolUUID, CompositeID: r.CompositeID, LeaseID: r.LeaseID,
		ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation}, 0); err != nil {
		t.Fatal(err)
	}
	var pvcs corev1.PersistentVolumeClaimList
	var pvs corev1.PersistentVolumeList
	if err := k.List(t.Context(), &pvcs, client.InNamespace(source.Config.Namespace)); err != nil {
		t.Fatal(err)
	}
	if err := k.List(t.Context(), &pvs); err != nil {
		t.Fatal(err)
	}
	if len(pvcs.Items) != 3 || len(pvs.Items) != 2 {
		t.Fatal("closure changed externally owned resources", len(pvcs.Items), len(pvs.Items))
	}
	for _, pvc := range pvcs.Items {
		if pvc.DeletionTimestamp != nil || len(pvc.Finalizers) != 0 {
			t.Fatal("provider changed source/outer ownership", pvc.Name)
		}
	}
}
