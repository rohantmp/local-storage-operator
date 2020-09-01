package common

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ContainsFinalizer checks if finalizer is found in metadata
func ContainsFinalizer(metadata metav1.ObjectMeta, finalizer string) bool {
	for _, f := range metadata.Finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

// SetFinalizer either adds or removes finalizer depending on `setFinalizer`
// it returns a bool indicating whether the metadata was changed or not.
func SetFinalizer(metadata *metav1.ObjectMeta, finalizer string, setFinalizer bool) bool {
	if ContainsFinalizer(*metadata, finalizer) {
		if setFinalizer {
			return false // not changed
		} else {
			newList := make([]string, 0)
			for i, f := range metadata.Finalizers {
				if f == finalizer {
					newList = append(metadata.Finalizers[:i], metadata.Finalizers[i+1:]...)
				}
			}
			metadata.Finalizers = newList
			return true
		}
	}

	// doesn't contain finalizer
	if setFinalizer {
		metadata.Finalizers = append(metadata.Finalizers, finalizer)
	} else {
		return false // not changed
	}

	return true
}

// GetBoundOrReleasedPVs owned by the object
func GetBoundOrReleasedPVs(obj runtime.Object, c client.Client) ([]corev1.PersistentVolume, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {

		return []corev1.PersistentVolume{}, fmt.Errorf("could not get object metadata accessor from obj: %+v", obj)
	}

	name := accessor.GetName()
	namespace := accessor.GetNamespace()
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if len(name) == 0 || len(namespace) == 0 || len(kind) == 0 {
		return []corev1.PersistentVolume{}, fmt.Errorf("name: %q, namespace: %q, or  kind: %q is empty for obj: %+v", name, namespace, kind, obj)
	}

	// fetch PVs that match the LocalVolumeSet
	pvList := &corev1.PersistentVolumeList{}
	ownerSelector := client.MatchingLabels{
		PVOwnerKindLabel:      kind,
		PVOwnerNameLabel:      name,
		PVOwnerNamespaceLabel: namespace,
	}
	err = c.List(context.TODO(), pvList, ownerSelector)
	if err != nil {
		return []corev1.PersistentVolume{}, fmt.Errorf("failed to list persistent volumes: %w", err)
	}
	var boundPVs = make([]corev1.PersistentVolume, 0, len(pvList.Items))

	for _, pv := range pvList.Items {
		// ensure ownership
		ownerKind, found := pv.Labels[PVOwnerKindLabel]
		if !found {
			return boundPVs, fmt.Errorf("PV owner info not found in PV.Labels: %+v", pv.Labels)
		}
		kindMatches := ownerKind == kind

		ownerName, found := pv.Labels[PVOwnerNameLabel]
		if !found {
			return boundPVs, fmt.Errorf("PV owner info not found in PV.Labels: %+v", pv.Labels)
		}
		nameMatches := ownerName == name

		ownerNamespace, found := pv.Labels[PVOwnerNamespaceLabel]
		if !found {
			return boundPVs, fmt.Errorf("PV owner info not found in PV.Labels: %+v", pv.Labels)
		}
		namespaceMatches := ownerNamespace == namespace

		// append to list if bound and owned
		if (pv.Status.Phase == corev1.VolumeBound || pv.Status.Phase == corev1.VolumeReleased) && kindMatches && nameMatches && namespaceMatches {
			boundPVs = append(boundPVs, pv)
		}
	}
	return boundPVs, nil
}
