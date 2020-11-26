package common

import (
	localv1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// LocalVolumeOwnerNameForPV stores name of LocalVolume that created this PV
	LocalVolumeOwnerNameForPV = "storage.openshift.com/local-volume-owner-name"
	// LocalVolumeOwnerNamespaceForPV stores namespace of LocalVolume that created this PV
	LocalVolumeOwnerNamespaceForPV = "storage.openshift.com/local-volume-owner-namespace"

	// PVOwnerKindLabel stores the namespace of the CR that created this PV
	PVOwnerKindLabel = "storage.openshift.com/owner-kind"
	// PVOwnerNameLabel stores the name of the CR that created this PV
	PVOwnerNameLabel = "storage.openshift.com/owner-name"
	// PVOwnerNamespaceLabel stores the namespace of the CR that created this PV
	PVOwnerNamespaceLabel = "storage.openshift.com/owner-namespace"
)

// GetPVOwnerSelector returns selector for selecting pvs owned by given volume
func GetPVOwnerSelector(lv *localv1.LocalVolume) labels.Selector {
	pvOwnerLabels := labels.Set{
		LocalVolumeOwnerNameForPV:      lv.Name,
		LocalVolumeOwnerNamespaceForPV: lv.Namespace,
	}
	return labels.SelectorFromSet(pvOwnerLabels)
}
