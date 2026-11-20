// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
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

func TestCommittedSourceCordonDeletionTeardown(t *testing.T) {
	for _, staleSource := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact-prior-source", true: "replacement-node"}[staleSource], func(t *testing.T) {
			p, source, k, _, composite := sourceProviderFixture(t)
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
			var node corev1.Node
			if err := k.Get(t.Context(), client.ObjectKey{Name: o.NodeID}, &node); err != nil {
				t.Fatal(err)
			}
			node.Spec.Unschedulable = true
			if staleSource {
				node.UID = "replacement-node-uid"
			}
			if err := k.Update(t.Context(), &node); err != nil {
				t.Fatal(err)
			}
			cr := &unstructured.Unstructured{}
			cr.SetAPIVersion("storage.infinitydb.io/v1alpha1")
			cr.SetKind("CompositeVolume")
			if err := k.Get(t.Context(), client.ObjectKey{Name: composite}, cr); err != nil {
				t.Fatal(err)
			}
			if err := k.Delete(t.Context(), cr); err != nil {
				t.Fatal(err)
			}
			if _, err := source.Resolve(t.Context(), composite, r.CapacityBytes); err == nil {
				t.Fatal("new admission tolerated deletion/cordon")
			}
			if _, err := p.Renew(t.Context(), authority, "renew", o); err == nil {
				t.Fatal("renew admitted on deleting source")
			}
			if _, err := p.Acquire(t.Context(), authority, "acquire", o, 0); err == nil {
				t.Fatal("acquire replay admitted on deleting source")
			}
			if err := p.ValidateOpen(t.Context(), proof{owner: o, open: true}, o, uint64(r.CapacityBytes), 253, 9); err == nil {
				t.Fatal("validation admitted on deleting source")
			}
			if staleSource {
				if err := p.ReleaseNode(t.Context(), authority, "release", o, "receipt"); err == nil {
					t.Fatal("teardown accepted replaced source UID")
				}
				if err := p.BeforePhysical(r.Handle, ""); !errors.Is(err, ErrHeld) {
					t.Fatal("failed teardown dropped hold", err)
				}
				return
			}
			if _, err := p.Lookup(t.Context(), r.PoolUUID, r.LeaseID); err != nil {
				t.Fatal("cordon/deletion blocked committed receipt", err)
			}
			stale := o
			stale.Epoch++
			if err := p.ReleaseNode(t.Context(), authority, "stale", stale, "receipt"); !errors.Is(err, ErrStale) {
				t.Fatal("teardown accepted stale owner", err)
			}
			if err := p.ReleaseNode(t.Context(), authority, "release", o, "receipt"); err != nil {
				t.Fatal("exact independently verified teardown was blocked", err)
			}
			if err := p.ReleaseAllocation(t.Context(), "close", Allocation{PoolUUID: r.PoolUUID, CompositeID: r.CompositeID,
				LeaseID: r.LeaseID, ResourceID: r.ResourceID(), StableDeviceID: r.StableDeviceID, Generation: r.Generation}, o.Epoch); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func sourceFixture(t *testing.T) (KubernetesSource, *unstructured.Unstructured, []client.Object) {
	t.Helper()
	raw, err := os.ReadFile("testdata/backing-pvcs-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	v := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.infinitydb.io/v1alpha1", "kind": "CompositeVolume",
		"spec": map[string]any{"capacity": int64(8192), "cacheBytes": int64(4096),
			"cacheParameters": map[string]any{"provider": "vg-uuid", "poolID": OrdinaryPool, "requestID": "reserve"},
			"topology":        map[string]any{backingPrefix + "node": "canary-node"}},
	}}
	v.SetName("dmstack-" + strings.Repeat("a", 52))
	v.SetUID("composite-uid")
	v.SetResourceVersion("1")
	v.SetFinalizers([]string{backingFinalizer})
	v.SetAnnotations(map[string]string{BackingJournalKey: string(raw), backingPrefix + "selected-node": "canary-node"})
	j, err := parseBacking(v)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DynamicConfig{NodeName: "canary-node", NodeUID: "node-uid", Namespace: "canary-backing",
		NamespaceUID: "namespace-uid", CacheClass: "cache", CacheClassUID: "cache-class-uid",
		VolumeGroup: "containerstorage", PoolUUID: "vg-uuid", Bytes: 4096}
	objects := []client.Object{v,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cfg.NodeName, UID: types.UID(cfg.NodeUID)}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.Namespace, UID: types.UID(cfg.NamespaceUID),
			Labels: map[string]string{backingPrefix + "backing-namespace": "v1"}}}}
	for _, s := range []struct {
		role, driver string
		n            int64
		pin          backingClaim
	}{{"origin", "disk.csi.azure.com", 8192, j.Origin}, {"cache", "localdisk.csi.acstor.io", 4096, j.Cache}} {
		mode := corev1.PersistentVolumeBlock
		binding := storagev1.VolumeBindingWaitForFirstConsumer
		q := *resource.NewQuantity(s.n, resource.BinarySI)
		annotations := map[string]string{backingPrefix + "composite-handle": j.Owner, backingPrefix + "backing-role": s.role,
			"volume.kubernetes.io/selected-node": cfg.NodeName, "localdisk.csi.acstor.io/accept-ephemeral-storage": "true"}
		objects = append(objects,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: s.role, UID: types.UID(s.pin.ClassUID)},
				Provisioner: s.driver, VolumeBindingMode: &binding},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: s.pin.Name, Namespace: cfg.Namespace,
				UID: types.UID(s.pin.UID), Annotations: annotations, Finalizers: []string{backingFinalizer}},
				Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &s.role, VolumeName: s.pin.PVName, VolumeMode: &mode,
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: q}}},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: q}}},
			&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: s.pin.PVName, UID: types.UID(s.pin.PVUID)},
				Spec: corev1.PersistentVolumeSpec{StorageClassName: s.role, VolumeMode: &mode,
					AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Capacity:               corev1.ResourceList{corev1.ResourceStorage: q},
					ClaimRef:               &corev1.ObjectReference{Name: s.pin.Name, Namespace: cfg.Namespace, UID: types.UID(s.pin.UID)},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: s.driver, VolumeHandle: s.pin.Handle}},
					NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{cfg.NodeName}}}},
					}}}},
				Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
		)
	}
	return KubernetesSource{Config: cfg}, v, objects
}

func TestAuthoritativeBackingJournalCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*unstructured.Unstructured, []client.Object)
	}{
		{"compatible-v1", nil},
		{"node-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[1].SetUID("replacement") }},
		{"namespace-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[2].SetUID("replacement") }},
		{"class-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[6].SetUID("replacement") }},
		{"pvc-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[7].SetUID("replacement") }},
		{"pv-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[8].SetUID("replacement") }},
		{"origin-incarnation", func(_ *unstructured.Unstructured, o []client.Object) { o[5].SetUID("replacement") }},
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
		{"missing-finalizer", func(v *unstructured.Unstructured, _ []client.Object) { v.SetFinalizers(nil) }},
		{"unknown-version", func(v *unstructured.Unstructured, _ []client.Object) {
			a := v.GetAnnotations()
			a[backingPrefix+"backing-pvcs-v2"] = a[BackingJournalKey]
			v.SetAnnotations(a)
		}},
		{"unknown-field", func(v *unstructured.Unstructured, _ []client.Object) {
			a := v.GetAnnotations()
			a[BackingJournalKey] = strings.Replace(a[BackingJournalKey], `"owner":`, `"version":2,"owner":`, 1)
			v.SetAnnotations(a)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k, v, objects := sourceFixture(t)
			if tc.mutate != nil {
				tc.mutate(v, objects)
			}
			s := runtime.NewScheme()
			_ = corev1.AddToScheme(s)
			_ = storagev1.AddToScheme(s)
			k.Reader = fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()
			c, err := k.Resolve(t.Context(), v.GetName(), k.Config.Bytes)
			if tc.mutate == nil {
				if err != nil || c.CompositeUID != "composite-uid" || c.Fingerprint == "" {
					t.Fatal(c, err)
				}
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: k.Config.Namespace},
					Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "cache", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: objects[7].GetName()}}}}}}
				if err := k.Reader.(client.Client).Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if _, err := k.Resolve(t.Context(), v.GetName(), k.Config.Bytes); err == nil {
					t.Fatal("consumer admitted")
				}
			} else if err == nil {
				t.Fatal("invalid source admitted", c)
			}
			if tc.name == "deleting" {
				if _, err := k.ResolveCommitted(t.Context(), v.GetName(), k.Config.Bytes); err != nil {
					t.Fatal("same deleting CR cannot recover committed receipt", err)
				}
			} else if tc.mutate != nil {
				if _, err := k.ResolveCommitted(t.Context(), v.GetName(), k.Config.Bytes); err == nil {
					t.Fatal("receipt recovery bypassed source identity")
				}
			}
		})
	}
}
