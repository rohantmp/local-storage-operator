package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"

	framework "github.com/operator-framework/operator-sdk/pkg/test"

	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
)

// name is only for logging
func eventuallyDelete(t *testing.T, obj runtime.Object, name string) {
	f := framework.Global
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	matcher := gomega.NewWithT(t)
	matcher.Eventually(func() error {
		t.Logf("deleting %v %q", kind, name)
		err := f.Client.Delete(context.TODO(), obj)
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}, time.Minute*1, time.Second*5).ShouldNot(gomega.HaveOccurred(), "deleting %v %q", kind, name)

}

func eventuallyFindPVs(t *testing.T, f *framework.Framework, lvset localv1alpha1.LocalVolumeSet, expectedPVs int) {
	matcher := gomega.NewWithT(t)
	matcher.Eventually(func() []corev1.PersistentVolume {
		pvList := &corev1.PersistentVolumeList{}
		t.Log(fmt.Sprintf("waiting for %d PVs to be created with StorageClass: %q", expectedPVs, lvset.Spec.StorageClassName))
		matcher.Eventually(func() error {
			return f.Client.List(context.TODO(), pvList)
		}).ShouldNot(gomega.HaveOccurred())
		matchedPVs := make([]corev1.PersistentVolume, 0)
		for _, pv := range pvList.Items {
			if pv.Spec.StorageClassName == lvset.Spec.StorageClassName {
				matchedPVs = append(matchedPVs, pv)
			}
		}
		return matchedPVs
	}).Should(gomega.HaveLen(2), "checking number of PVs for for storageclass: %q", lvset.Spec.StorageClassName)

}
