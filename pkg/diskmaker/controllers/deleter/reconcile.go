package deleter

import (
	"context"
	"fmt"

	"github.com/openshift/local-storage-operator/pkg/common"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	provCommon "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"

	// staticProvisioner "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"	"context"
	// "errors"
	// "fmt"
	// "hash/fnv"
	// "os"
	// "path"
	// "path/filepath"
	// "time"
	// "github.com/go-logr/logr"
	// localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	// "github.com/openshift/local-storage-operator/pkg/common"
	// "github.com/openshift/local-storage-operator/pkg/diskmaker"
	// "github.com/openshift/local-storage-operator/pkg/internal"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	// storagev1 "k8s.io/api/storage/v1"
	// kerrors "k8s.io/apimachinery/pkg/api/errors"
	// "k8s.io/apimachinery/pkg/types"
	// "k8s.io/apimachinery/pkg/util/sets"
	// "sigs.k8s.io/controller-runtime/pkg/reconcile"
	// provCommon "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"
	staticProvisioner "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"
)

// Reconcile reads that state of the cluster for a LocalVolumeSet object and makes changes based on the state read
// and what is in the LocalVolumeSet.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileDeleter) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling LocalVolumeSet")
	// enqueue if cache is not initialized
	// and if any pv has phase == Releaseds

	// mux for runtimeConfig access
	r.provisionerMux.Lock()
	defer r.provisionerMux.Unlock()

	// get associated provisioner config
	cm := &corev1.ConfigMap{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: common.ProvisionerConfigMapName, Namespace: request.Namespace}, cm)
	if err != nil {
		reqLogger.Error(err, "could not get provisioner configmap")
		return reconcile.Result{}, err
	}

	// read provisioner config
	provisionerConfig := staticProvisioner.ProvisionerConfiguration{}
	staticProvisioner.ConfigMapDataToVolumeConfig(cm.Data, &provisionerConfig)

	r.runtimeConfig.DiscoveryMap = provisionerConfig.StorageClassConfig
	r.runtimeConfig.NodeLabelsForPV = provisionerConfig.NodeLabelsForPV
	r.runtimeConfig.Namespace = request.Namespace
	r.runtimeConfig.SetPVOwnerRef = provisionerConfig.SetPVOwnerRef
	r.runtimeConfig.Name = common.GetProvisionedByValue(*r.runtimeConfig.Node)

	// ignored by our implementation of static-provisioner,
	// but not by deleter (if applicable)
	r.runtimeConfig.UseNodeNameOnly = provisionerConfig.UseNodeNameOnly
	r.runtimeConfig.MinResyncPeriod = provisionerConfig.MinResyncPeriod
	r.runtimeConfig.UseAlphaAPI = provisionerConfig.UseAlphaAPI
	r.runtimeConfig.LabelsForPV = provisionerConfig.LabelsForPV

	// initialize the pv cache
	// initialize the deleter's pv cache on the first run
	if !r.firstRunOver {
		log.Info("initializing PV cache")
		pvList := &corev1.PersistentVolumeList{}
		err := r.client.List(context.TODO(), pvList)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to initialize PV cache")
		}
		for _, pv := range pvList.Items {
			// skip non-owned PVs
			name, found := pv.Annotations[provCommon.AnnProvisionedBy]
			if !found || name != r.runtimeConfig.Name {
				continue
			}
			addOrUpdatePV(r.runtimeConfig, pv)
		}

		r.firstRunOver = true
	}

	// get the lock
	// run the deleter
	//      for each pv with phase == released, mark cleanup running and run cleanup.
	r.deleter.DeletePVs()
	return reconcile.Result{}, nil
}
