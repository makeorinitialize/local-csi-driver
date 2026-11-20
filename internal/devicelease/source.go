// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package devicelease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	backingPrefix     = "dmstack.csi.infinitydb.io/"
	BackingJournalKey = backingPrefix + "backing-pvcs-v1"
	backingFinalizer  = backingPrefix + "backing-pvcs"
)

var compositeName = regexp.MustCompile(`^dmstack-[a-z2-7]{52}$`)

// These JSON tags are the public backing-pvcs-v1 annotation contract, not
// guesses at the facade Go field names. In particular capacity is NOT the
// journal's originBytes. No dependency on the private facade repository.
type backingConfig struct {
	Namespace          string `json:"namespace"`
	NamespaceUID       string `json:"namespaceUID"`
	NodeName           string `json:"nodeName"`
	NodeUID            string `json:"nodeUID"`
	OriginStorageClass string `json:"originStorageClass"`
	CacheStorageClass  string `json:"cacheStorageClass"`
	OriginBytes        int64  `json:"originBytes"`
	CacheBytes         int64  `json:"cacheBytes"`
	OriginDriver       string `json:"originDriver,omitempty"`
	CacheDriver        string `json:"cacheDriver,omitempty"`
}
type backingClaim struct {
	Name     string                      `json:"name"`
	UID      string                      `json:"uid,omitempty"`
	ClassUID string                      `json:"classUID"`
	Binding  storagev1.VolumeBindingMode `json:"binding"`
	PVName   string                      `json:"pvName,omitempty"`
	PVUID    string                      `json:"pvUID,omitempty"`
	Handle   string                      `json:"handle,omitempty"`
}
type backingJournal struct {
	Config backingConfig `json:"config"`
	Owner  string        `json:"owner"`
	Origin backingClaim  `json:"origin"`
	Cache  backingClaim  `json:"cache"`
}

// KubernetesSource must use a separate uncached, read-only client and
// credentials. API availability is required only inside the opt-in domain.
// Namespace admission/RBAC must prohibit backing PVC consumers and writes by
// workloads; absence in a list alone cannot prevent a concurrent Pod create.
type KubernetesSource struct {
	Reader client.Reader
	Config DynamicConfig
}

func parseBacking(v *unstructured.Unstructured) (backingJournal, error) {
	var j backingJournal
	for k := range v.GetAnnotations() {
		if strings.HasPrefix(k, backingPrefix+"backing-pvcs-") && k != BackingJournalKey {
			return j, ErrUncertain
		}
	}
	d := json.NewDecoder(strings.NewReader(v.GetAnnotations()[BackingJournalKey]))
	d.DisallowUnknownFields()
	if err := d.Decode(&j); err != nil {
		return j, ErrUncertain
	}
	if d.Decode(new(any)) != io.EOF || !slices.Contains(v.GetFinalizers(), backingFinalizer) {
		return j, ErrUncertain
	}
	return j, nil
}

func (k KubernetesSource) Resolve(ctx context.Context, composite string, n int64) (Candidate, error) {
	return k.resolve(ctx, composite, n, false)
}

func (k KubernetesSource) ResolveCommitted(ctx context.Context, composite string, n int64) (Candidate, error) {
	return k.resolve(ctx, composite, n, true)
}

func (k KubernetesSource) resolve(ctx context.Context, composite string, n int64, receiptOnly bool) (Candidate, error) {
	fail := func() (Candidate, error) { return Candidate{}, ErrUncertain }
	c := k.Config
	if !c.valid() || k.Reader == nil || !compositeName.MatchString(composite) || n != c.Bytes {
		return fail()
	}
	v := &unstructured.Unstructured{}
	v.SetGroupVersionKind(schema.GroupVersionKind{Group: "storage.infinitydb.io", Version: "v1alpha1", Kind: "CompositeVolume"})
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: composite}, v); err != nil {
		return Candidate{}, err
	}
	j, err := parseBacking(v)
	if err != nil || v.GetUID() == "" || v.GetResourceVersion() == "" || !receiptOnly && v.GetDeletionTimestamp() != nil ||
		v.GetNamespace() != "" || len(v.GetOwnerReferences()) != 0 ||
		j.Owner != composite+"."+string(v.GetUID()) ||
		v.GetAnnotations()[backingPrefix+"selected-node"] != c.NodeName {
		return fail()
	}
	cfg := j.Config
	if !validDrivers(cfg.OriginDriver, cfg.CacheDriver) ||
		originDriver(cfg.OriginDriver) != c.OriginDriverName() || cacheDriver(cfg.CacheDriver) != c.CacheDriverName() ||
		cfg.Namespace != c.Namespace || cfg.NamespaceUID != c.NamespaceUID || cfg.NodeName != c.NodeName ||
		cfg.NodeUID != c.NodeUID || cfg.CacheStorageClass != c.CacheClass || j.Cache.ClassUID != c.CacheClassUID ||
		cfg.CacheBytes != n || cfg.OriginBytes <= 0 || cfg.OriginBytes%512 != 0 ||
		cfg.OriginStorageClass == "" || cfg.OriginStorageClass == cfg.CacheStorageClass {
		return fail()
	}
	originBytes, _, _ := unstructured.NestedInt64(v.Object, "spec", "capacity")
	cacheBytes, _, _ := unstructured.NestedInt64(v.Object, "spec", "cacheBytes")
	provider, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "provider")
	pool, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "poolID")
	request, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "requestID")
	topologyNode, _, _ := unstructured.NestedString(v.Object, "spec", "topology", backingPrefix+"node")
	if originBytes != cfg.OriginBytes || cacheBytes != cfg.CacheBytes || provider != c.PoolUUID || pool != OrdinaryPool ||
		topologyNode != c.NodeName || request == "" {
		return fail()
	}
	var namespace corev1.Namespace
	var node corev1.Node
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: c.Namespace}, &namespace); err != nil {
		return Candidate{}, err
	}
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: c.NodeName}, &node); err != nil {
		return Candidate{}, err
	}
	if string(namespace.UID) != c.NamespaceUID || namespace.DeletionTimestamp != nil ||
		namespace.Labels[backingPrefix+"backing-namespace"] != "v1" || len(namespace.OwnerReferences) != 0 ||
		string(node.UID) != c.NodeUID || node.DeletionTimestamp != nil || !receiptOnly && node.Spec.Unschedulable {
		return fail()
	}
	for _, side := range []struct {
		role, class, driver string
		bytes               int64
		pin                 backingClaim
	}{
		{"origin", cfg.OriginStorageClass, originDriver(cfg.OriginDriver), cfg.OriginBytes, j.Origin},
		{"cache", cfg.CacheStorageClass, cacheDriver(cfg.CacheDriver), cfg.CacheBytes, j.Cache},
	} {
		if err := k.checkBinding(ctx, j.Owner, &node, side.role, side.class, side.driver, side.bytes, side.pin); err != nil {
			return Candidate{}, err
		}
	}
	vg, lv, ok := strings.Cut(j.Cache.Handle, "#")
	if !ok || vg != c.VolumeGroup || !pvcLV.MatchString(lv) {
		return fail()
	}
	var pods corev1.PodList
	if err := k.Reader.List(ctx, &pods, client.InNamespace(c.Namespace)); err != nil {
		return Candidate{}, err
	}
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if claim := volume.PersistentVolumeClaim; claim != nil && (claim.ClaimName == j.Cache.Name || claim.ClaimName == j.Origin.Name) {
				return fail()
			}
		}
	}
	// Re-read the authoritative CR, not an informer snapshot. Deletion or
	// identity mutation during the dependency reads cannot be accepted.
	again := v.DeepCopy()
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: composite}, again); err != nil {
		return Candidate{}, err
	}
	if again.GetUID() != v.GetUID() || again.GetResourceVersion() != v.GetResourceVersion() ||
		!receiptOnly && again.GetDeletionTimestamp() != nil {
		return fail()
	}
	// Semantic default equivalence for fingerprints only; never rewrite the
	// authoritative annotation or change the old omitted-field serialization.
	if j.Config.OriginDriver == OfficialOriginDriver {
		j.Config.OriginDriver = ""
	}
	if j.Config.CacheDriver == OfficialCacheDriver {
		j.Config.CacheDriver = ""
	}
	b, _ := json.Marshal(j)
	return Candidate{CompositeID: composite, CompositeUID: string(v.GetUID()), VolumeHandle: j.Cache.Handle,
		Fingerprint: fmt.Sprintf("%x", sha256.Sum256(b)), RequestID: request}, nil
}

func (k KubernetesSource) checkBinding(ctx context.Context, owner string, node *corev1.Node, role, class, driver string, n int64, pin backingClaim) error {
	hash := sha256.Sum256([]byte(owner))
	if pin.Name != fmt.Sprintf("dmstack-%x-%s", hash[:20], role) || pin.UID == "" || pin.ClassUID == "" ||
		pin.PVName == "" || pin.PVUID == "" || pin.Handle == "" {
		return ErrUncertain
	}
	var sc storagev1.StorageClass
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: class}, &sc); err != nil {
		return err
	}
	if string(sc.UID) != pin.ClassUID || sc.DeletionTimestamp != nil || sc.Provisioner != driver ||
		sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != pin.Binding ||
		(pin.Binding != storagev1.VolumeBindingImmediate && pin.Binding != storagev1.VolumeBindingWaitForFirstConsumer) {
		return ErrUncertain
	}
	if len(sc.AllowedTopologies) != 0 {
		match := false
		for _, term := range sc.AllowedTopologies {
			ok := len(term.MatchLabelExpressions) > 0
			for _, e := range term.MatchLabelExpressions {
				ok = ok && slices.Contains(e.Values, node.Labels[e.Key])
			}
			match = match || ok
		}
		if !match {
			return ErrUncertain
		}
	}
	if role == "cache" {
		vg := sc.Parameters["volumeGroup"]
		if vg == "" {
			vg = "containerstorage"
		}
		if vg != k.Config.VolumeGroup {
			return ErrUncertain
		}
	}
	var pvc corev1.PersistentVolumeClaim
	var pv corev1.PersistentVolume
	if err := k.Reader.Get(ctx, client.ObjectKey{Namespace: k.Config.Namespace, Name: pin.Name}, &pvc); err != nil {
		return err
	}
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: pin.PVName}, &pv); err != nil {
		return err
	}
	selected := pvc.Annotations["volume.kubernetes.io/selected-node"]
	q := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	claimCapacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if string(pvc.UID) != pin.UID || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != pin.PVName || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != class ||
		pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != corev1.PersistentVolumeBlock ||
		!slices.Equal(pvc.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		q.CmpInt64(n) != 0 || len(pvc.Spec.Resources.Limits) != 0 || pvc.Spec.Selector != nil ||
		pvc.Spec.DataSource != nil || pvc.Spec.DataSourceRef != nil || pvc.Spec.VolumeAttributesClassName != nil ||
		pvc.Annotations[backingPrefix+"composite-handle"] != owner || pvc.Annotations[backingPrefix+"backing-role"] != role ||
		len(pvc.OwnerReferences) != 0 || !slices.Contains(pvc.Finalizers, backingFinalizer) ||
		selected != "" && selected != node.Name ||
		pin.Binding == storagev1.VolumeBindingWaitForFirstConsumer && selected != node.Name ||
		role == "cache" && pvc.Annotations["localdisk.csi.acstor.io/accept-ephemeral-storage"] != "true" ||
		string(pv.UID) != pin.PVUID || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound ||
		pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.CSI == nil || pv.Spec.CSI.Driver != driver || pv.Spec.CSI.VolumeHandle != pin.Handle || pv.Spec.CSI.ReadOnly ||
		pv.Spec.StorageClassName != class || pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != corev1.PersistentVolumeBlock ||
		!slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) || capacity.CmpInt64(n) != 0 || claimCapacity.Cmp(capacity) != 0 ||
		!sourceNodeMatches(pv.Spec.NodeAffinity, node) {
		return ErrUncertain
	}
	return nil
}

func sourceNodeMatches(a *corev1.VolumeNodeAffinity, node *corev1.Node) bool {
	if a == nil || a.Required == nil {
		return false
	}
	for _, term := range a.Required.NodeSelectorTerms {
		ok := len(term.MatchExpressions)+len(term.MatchFields) > 0
		for _, e := range term.MatchExpressions {
			ok = ok && e.Operator == corev1.NodeSelectorOpIn && slices.Contains(e.Values, node.Labels[e.Key])
		}
		for _, e := range term.MatchFields {
			ok = ok && e.Key == "metadata.name" && e.Operator == corev1.NodeSelectorOpIn && slices.Contains(e.Values, node.Name)
		}
		if ok {
			return true
		}
	}
	return false
}

// DecodeDynamicConfig rejects unrecognized administrator configuration too.
func DecodeDynamicConfig(data []byte) (DynamicConfig, error) {
	var c DynamicConfig
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if d.Decode(new(any)) != io.EOF || !c.valid() {
		return c, ErrUncertain
	}
	return c, nil
}
