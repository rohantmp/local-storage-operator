package nodedaemon

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
)

func (r *DaemonReconciler) aggregateDeamonInfo(request reconcile.Request) (localv1alpha1.LocalVolumeSetList, []corev1.Toleration, []metav1.OwnerReference, *corev1.NodeSelector, error) {
	//list
	lvSetList := localv1alpha1.LocalVolumeSetList{}
	err := r.client.List(context.TODO(), &lvSetList, client.InNamespace(request.Namespace))
	if err != nil {
		return localv1alpha1.LocalVolumeSetList{}, []corev1.Toleration{}, []metav1.OwnerReference{}, nil, fmt.Errorf("could not fetch localvolumeset link: %w", err)
	}

	lvSets := lvSetList.Items
	tolerations, ownerRefs, terms := extractLVSetInfo(lvSets)
	var nodeSelector *corev1.NodeSelector = nil
	if len(terms) > 0 {
		nodeSelector = &corev1.NodeSelector{NodeSelectorTerms: terms}
	}

	return lvSetList, tolerations, ownerRefs, nodeSelector, err
}

func extractLVSetInfo(lvsets []localv1alpha1.LocalVolumeSet) ([]corev1.Toleration, []metav1.OwnerReference, []corev1.NodeSelectorTerm) {
	tolerations := make([]corev1.Toleration, 0)
	ownerRefs := make([]metav1.OwnerReference, 0)
	terms := make([]corev1.NodeSelectorTerm, 0)
	// if any one of the lvset nodeSelectors are nil, the terms should be empty to indicate matchAllNodes
	matchAllNodes := false

	// sort so that changing order doesn't cause unneccesary updates
	sort.SliceStable(lvsets, func(i, j int) bool {
		a := fmt.Sprintf("%s-%s", lvsets[i].GetName(), lvsets[i].Spec.StorageClassName)
		b := fmt.Sprintf("%s-%s", lvsets[j].GetName(), lvsets[j].Spec.StorageClassName)
		return strings.Compare(a, b) == -1
	})
	for _, lvset := range lvsets {
		tolerations = append(tolerations, lvset.Spec.Tolerations...)

		falseVar := false
		ownerRefs = append(ownerRefs, metav1.OwnerReference{
			UID:                lvset.GetUID(),
			Name:               lvset.GetName(),
			APIVersion:         lvset.APIVersion,
			Kind:               lvset.Kind,
			Controller:         &falseVar,
			BlockOwnerDeletion: &falseVar,
		})
		if lvset.Spec.NodeSelector != nil {
			terms = append(terms, lvset.Spec.NodeSelector.NodeSelectorTerms...)
		} else {
			matchAllNodes = true
		}

	}
	if matchAllNodes {
		fmt.Printf("AZF: matchAllNodes")
		terms = make([]corev1.NodeSelectorTerm, 0)
	}

	return tolerations, ownerRefs, terms
}
