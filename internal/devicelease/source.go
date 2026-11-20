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
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sourcePrefix      = "dmstack.csi.infinitydb.io/"
	ExternalSourceKey = sourcePrefix + "external-source-pvcs-v1"
)

var compositeName = regexp.MustCompile(`^dmstack-[a-z2-7]{52}$`)

// Field order/tags are the pinned external-source-pvcs-v1 contract. Sources
// belong to the user; none of these records confers creation/deletion rights.
type externalConfig struct {
	Namespace    string `json:"namespace"`
	NamespaceUID string `json:"namespaceUID"`
	NodeName     string `json:"nodeName"`
	NodeUID      string `json:"nodeUID"`
	OriginDriver string `json:"originDriver,omitempty"`
	CacheDriver  string `json:"cacheDriver,omitempty"`
}
type outerClaim struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}
type externalClaim struct {
	Name      string `json:"name"`
	UID       string `json:"uid"`
	PVName    string `json:"pvName"`
	PVUID     string `json:"pvUID"`
	ClassName string `json:"className"`
	ClassUID  string `json:"classUID"`
	Driver    string `json:"driver"`
	Handle    string `json:"handle"`
	Bytes     int64  `json:"bytes"`
}
type externalJournal struct {
	Version int            `json:"version"`
	Owner   string         `json:"owner"`
	Config  externalConfig `json:"config"`
	Outer   outerClaim     `json:"outer"`
	Origin  externalClaim  `json:"origin"`
	Cache   externalClaim  `json:"cache"`
}

func decodeExternal(raw string) (externalJournal, error) {
	var j externalJournal
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&j); err != nil || d.Decode(new(any)) != io.EOF || j.Version != 1 {
		return j, ErrUncertain
	}
	return j, nil
}

func parseExternal(v *unstructured.Unstructured) (externalJournal, error) {
	for key := range v.GetAnnotations() {
		if strings.HasPrefix(key, sourcePrefix+"backing-pvcs-") ||
			strings.HasPrefix(key, sourcePrefix+"external-source-pvcs-") && key != ExternalSourceKey {
			return externalJournal{}, ErrUncertain
		}
	}
	if slices.Contains(v.GetFinalizers(), sourcePrefix+"backing-pvcs") {
		return externalJournal{}, ErrUncertain
	}
	return decodeExternal(v.GetAnnotations()[ExternalSourceKey])
}

func validSourceName(s string) bool { return s != "" && len(validation.IsDNS1123Subdomain(s)) == 0 }

func (j externalJournal) pins(c DynamicConfig, composite, uid string, n int64) error {
	cfg := j.Config
	if !c.valid() || !compositeName.MatchString(composite) || uid == "" || j.Version != 1 || j.Owner != composite+"."+uid ||
		cfg.Namespace != c.Namespace || cfg.NamespaceUID != c.NamespaceUID || cfg.NodeName != c.NodeName ||
		cfg.NodeUID != c.NodeUID || !validDrivers(cfg.OriginDriver, cfg.CacheDriver) ||
		originDriver(cfg.OriginDriver) != c.OriginDriverName() || cacheDriver(cfg.CacheDriver) != c.CacheDriverName() ||
		!validSourceName(j.Outer.Name) || j.Outer.UID == "" || j.Origin.Name == j.Cache.Name ||
		j.Origin.Name == j.Outer.Name || j.Cache.Name == j.Outer.Name ||
		j.Cache.ClassName != c.CacheClass || j.Cache.ClassUID != c.CacheClassUID || n != c.Bytes || j.Cache.Bytes != n ||
		j.Origin.Driver != c.OriginDriverName() || j.Cache.Driver != c.CacheDriverName() {
		return ErrUncertain
	}
	for _, p := range []externalClaim{j.Origin, j.Cache} {
		if !validSourceName(p.Name) || !validSourceName(p.PVName) || !validSourceName(p.ClassName) ||
			p.UID == "" || p.PVUID == "" || p.ClassUID == "" || p.Driver == "" || p.Handle == "" ||
			p.Bytes <= 0 || p.Bytes%512 != 0 {
			return ErrUncertain
		}
	}
	vg, lv, ok := strings.Cut(j.Cache.Handle, "#")
	if !ok || vg != c.VolumeGroup || !pvcLV.MatchString(lv) {
		return ErrUncertain
	}
	return nil
}

func (j externalJournal) canonical() string {
	if j.Config.OriginDriver == OfficialOriginDriver {
		j.Config.OriginDriver = ""
	}
	if j.Config.CacheDriver == OfficialCacheDriver {
		j.Config.CacheDriver = ""
	}
	b, _ := json.Marshal(j)
	return string(b)
}

// KubernetesSource is a separate, uncached, read-only client. Outer annotations
// express references only, never placement, device identity or ownership proof.
type KubernetesSource struct {
	Reader client.Reader
	Config DynamicConfig
}

func (k KubernetesSource) Resolve(ctx context.Context, composite string, n int64) (Candidate, error) {
	fail := func() (Candidate, error) { return Candidate{}, ErrUncertain }
	c := k.Config
	if !c.valid() || k.Reader == nil || !compositeName.MatchString(composite) {
		return fail()
	}
	v := &unstructured.Unstructured{}
	v.SetGroupVersionKind(schema.GroupVersionKind{Group: "storage.infinitydb.io", Version: "v1alpha1", Kind: "CompositeVolume"})
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: composite}, v); err != nil {
		return Candidate{}, err
	}
	j, err := parseExternal(v)
	if err != nil || v.GetDeletionTimestamp() != nil || v.GetResourceVersion() == "" || v.GetNamespace() != "" ||
		j.pins(c, composite, string(v.GetUID()), n) != nil ||
		v.GetAnnotations()[sourcePrefix+"selected-node"] != c.NodeName {
		return fail()
	}
	originBytes, _, _ := unstructured.NestedInt64(v.Object, "spec", "capacity")
	cacheBytes, _, _ := unstructured.NestedInt64(v.Object, "spec", "cacheBytes")
	provider, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "provider")
	pool, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "poolID")
	request, _, _ := unstructured.NestedString(v.Object, "spec", "cacheParameters", "requestID")
	nodeName, _, _ := unstructured.NestedString(v.Object, "spec", "topology", sourcePrefix+"node")
	if originBytes != j.Origin.Bytes || cacheBytes != j.Cache.Bytes || provider != c.PoolUUID ||
		pool != OrdinaryPool || request == "" || nodeName != c.NodeName {
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
		string(node.UID) != c.NodeUID || node.DeletionTimestamp != nil || node.Spec.Unschedulable {
		return fail()
	}
	var outer corev1.PersistentVolumeClaim
	if err := k.Reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: j.Outer.Name}, &outer); err != nil {
		return Candidate{}, err
	}
	size := outer.Spec.Resources.Requests[corev1.ResourceStorage]
	if string(outer.UID) != j.Outer.UID || outer.DeletionTimestamp != nil ||
		outer.Annotations[sourcePrefix+"origin-pvc"] != j.Origin.Name ||
		outer.Annotations[sourcePrefix+"cache-pvc"] != j.Cache.Name ||
		outer.Spec.VolumeMode != nil && *outer.Spec.VolumeMode != corev1.PersistentVolumeFilesystem ||
		!slices.Equal(outer.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		size.CmpInt64(j.Origin.Bytes) != 0 {
		return fail()
	}
	for _, pin := range []externalClaim{j.Origin, j.Cache} {
		if err := k.checkBinding(ctx, &node, pin); err != nil {
			return Candidate{}, err
		}
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
	again := v.DeepCopy()
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: composite}, again); err != nil {
		return Candidate{}, err
	}
	if again.GetUID() != v.GetUID() || again.GetResourceVersion() != v.GetResourceVersion() || again.GetDeletionTimestamp() != nil {
		return fail()
	}
	var currentOuter corev1.PersistentVolumeClaim
	if err := k.Reader.Get(ctx, client.ObjectKeyFromObject(&outer), &currentOuter); err != nil {
		return Candidate{}, err
	}
	if currentOuter.UID != outer.UID || currentOuter.ResourceVersion != outer.ResourceVersion || currentOuter.DeletionTimestamp != nil {
		return fail()
	}
	snapshot := j.canonical()
	return Candidate{CompositeID: composite, CompositeUID: string(v.GetUID()), VolumeHandle: j.Cache.Handle,
		Fingerprint: fmt.Sprintf("%x", sha256.Sum256([]byte(snapshot))), RequestID: request, Snapshot: snapshot}, nil
}

func (k KubernetesSource) checkBinding(ctx context.Context, node *corev1.Node, pin externalClaim) error {
	var sc storagev1.StorageClass
	if err := k.Reader.Get(ctx, client.ObjectKey{Name: pin.ClassName}, &sc); err != nil {
		return err
	}
	if string(sc.UID) != pin.ClassUID || sc.DeletionTimestamp != nil || sc.Provisioner != pin.Driver {
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
	if pin.Driver == k.Config.CacheDriverName() {
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
	requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	claimCapacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if string(pvc.UID) != pin.UID || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != pin.PVName || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != pin.ClassName ||
		pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != corev1.PersistentVolumeBlock ||
		!slices.Equal(pvc.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		requested.CmpInt64(pin.Bytes) != 0 ||
		string(pv.UID) != pin.PVUID || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound ||
		pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name ||
		pv.Spec.CSI == nil || pv.Spec.CSI.Driver != pin.Driver || pv.Spec.CSI.VolumeHandle != pin.Handle || pv.Spec.CSI.ReadOnly ||
		pv.Spec.StorageClassName != pin.ClassName || pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != corev1.PersistentVolumeBlock ||
		!slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) || capacity.CmpInt64(pin.Bytes) != 0 || claimCapacity.Cmp(capacity) != 0 ||
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

func (s *ledger) validateSourceSnapshot() error {
	if s.Dynamic == nil || s.Dynamic.Source == nil || s.Dynamic.Source.Snapshot == "" {
		return nil // preserved legacy hold; never a new external-source admission
	}
	c := s.Dynamic.Source
	j, err := decodeExternal(c.Snapshot)
	if err != nil || j.pins(s.Dynamic.Config, c.CompositeID, c.CompositeUID, s.Dynamic.Config.Bytes) != nil ||
		j.canonical() != c.Snapshot || fmt.Sprintf("%x", sha256.Sum256([]byte(c.Snapshot))) != c.Fingerprint ||
		j.Cache.Handle != c.VolumeHandle || s.Canary == nil || s.Canary.VolumeHandle != c.VolumeHandle {
		return ErrUncertain
	}
	return nil
}

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
