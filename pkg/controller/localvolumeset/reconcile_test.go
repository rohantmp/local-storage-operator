package localvolumeset

import (
	"testing"

	localapis "github.com/openshift/local-storage-operator/pkg/apis"
	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	"github.com/openshift/local-storage-operator/pkg/common"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// This file should contain all the mocking required for LocalVolumeSetReconciler to run in a unit test.

const (
	testNamespace = "default"
)

func newFakeLocalVolumeSetReconciler(t *testing.T, objs ...runtime.Object) *LocalVolumeSetReconciler {
	scheme, err := localv1alpha1.SchemeBuilder.Build()
	assert.NoErrorf(t, err, "creating scheme")

	err = storagev1.AddToScheme(scheme)
	assert.NoErrorf(t, err, "adding storagev1 to scheme")

	err = localapis.AddToScheme(scheme)
	assert.NoErrorf(t, err, "adding localapis to scheme")

	err = corev1.AddToScheme(scheme)
	assert.NoErrorf(t, err, "adding corev1 to scheme")

	err = appsv1.AddToScheme(scheme)
	assert.NoErrorf(t, err, "adding appsv1 to scheme")

	client := fake.NewFakeClientWithScheme(scheme, objs...)

	return &LocalVolumeSetReconciler{
		client:   client,
		scheme:   scheme,
		lvSetMap: &common.StorageClassOwnerMap{},
	}
}
