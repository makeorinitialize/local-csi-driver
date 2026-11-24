// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package deviceuse

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/utils/ptr"

	pb "local-csi-driver/internal/deviceuse/usev1"
)

type KubernetesResolver struct {
	Scope  Scope
	Client kubernetes.Interface
	Native func(context.Context, *core.PersistentVolume, *core.Node) (Resource, error)
}

func (k KubernetesResolver) source(ctx context.Context, id *pb.UseIdentity) (*core.PersistentVolume, *core.Node, error) {
	if err := validateIdentity(k.Scope, id); err != nil {
		return nil, nil, err
	}
	bad := status.Error(codes.FailedPrecondition, "source is outside independently configured PV/node/namespace scope")
	node, err := k.Client.CoreV1().Nodes().Get(ctx, k.Scope.Node, metav1.GetOptions{})
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "source node unavailable")
	}
	ns, err := k.Client.CoreV1().Namespaces().Get(ctx, k.Scope.Namespace, metav1.GetOptions{})
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "source namespace unavailable")
	}
	pv, err := k.Client.CoreV1().PersistentVolumes().Get(ctx, id.Source.PvName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "source PV unavailable")
	}
	if string(node.UID) != k.Scope.NodeUID || node.DeletionTimestamp != nil ||
		string(ns.UID) != k.Scope.NamespaceUID || ns.DeletionTimestamp != nil ||
		string(pv.UID) != id.Source.PvUid || pv.DeletionTimestamp != nil || pv.Status.Phase != core.VolumeBound ||
		pv.Spec.CSI == nil || pv.Spec.CSI.Driver != id.Source.Driver || pv.Spec.CSI.VolumeHandle != id.Source.VolumeHandle ||
		pv.Spec.CSI.ReadOnly || ptr.Deref(pv.Spec.VolumeMode, core.PersistentVolumeFilesystem) != core.PersistentVolumeBlock ||
		len(pv.Spec.AccessModes) != 1 || pv.Spec.AccessModes[0] != core.ReadWriteOnce ||
		pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != k.Scope.Namespace || pv.Spec.ClaimRef.UID == "" {
		return nil, nil, bad
	}
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		matches, err := nodeaffinity.NewLazyErrorNodeSelector(pv.Spec.NodeAffinity.Required).Match(node)
		if err != nil || !matches {
			return nil, nil, bad
		}
	}
	ref := pv.Spec.ClaimRef
	pvc, err := k.Client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, "source PVC unavailable")
	}
	if pvc.UID != ref.UID || pvc.DeletionTimestamp != nil || pvc.Status.Phase != core.ClaimBound ||
		pvc.Spec.VolumeName != pv.Name || ptr.Deref(pvc.Spec.VolumeMode, core.PersistentVolumeFilesystem) != core.PersistentVolumeBlock ||
		len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != core.ReadWriteOnce {
		return nil, nil, bad
	}
	return pv, node, nil
}

func (k KubernetesResolver) CheckScope(ctx context.Context, id *pb.UseIdentity) error {
	_, _, err := k.source(ctx, id)
	return err
}

func (k KubernetesResolver) Resolve(ctx context.Context, id *pb.UseIdentity) (Resource, error) {
	pv, node, err := k.source(ctx, id)
	if err != nil {
		return Resource{}, err
	}
	if k.Native == nil {
		return Resource{}, status.Error(codes.Unavailable, "independent native resource observer required")
	}
	resource, err := k.Native(ctx, pv, node)
	if err != nil {
		return Resource{}, err
	}
	current, currentNode, err := k.source(ctx, id)
	if err != nil {
		return Resource{}, err
	}
	if current.ResourceVersion != pv.ResourceVersion || currentNode.ResourceVersion != node.ResourceVersion {
		return Resource{}, status.Error(codes.Aborted, "source changed during native identity observation")
	}
	return resource, nil
}
