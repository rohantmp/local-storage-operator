package localvolumeset

import (
	"context"
	"fmt"

	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	"github.com/openshift/local-storage-operator/pkg/common"
)

func (r *LocalVolumeSetReconciler) syncFinalizer(lvSet localv1alpha1.LocalVolumeSet) error {
	// finalizer should exist and be removed only when deleting
	setFinalizer := true

	// handle deletion
	if !lvSet.DeletionTimestamp.IsZero() {
		r.reqLogger.Info("deletionTimeStamp found, waiting for 0 bound PVs")
		// if obect is deleted, finalizer should be unset only when no boundPVs are found
		boundOrReleasedPVs, err := common.GetBoundOrReleasedPVs(&lvSet, r.client)
		if err != nil {
			return fmt.Errorf("could not list bound PVs: %w", err)
		}
		if len(boundOrReleasedPVs) == 0 {
			setFinalizer = false
			r.reqLogger.Info("no bound PVs found, removing finalizer")
		} else {
			pvNames := ""
			for _, pv := range boundOrReleasedPVs {
				pvNames += fmt.Sprintf(" %v", pv.Name)
			}
			r.reqLogger.Info("bound/released PVs found, not removing finalizer", "pvNames", pvNames)
		}
	}

	// update finalizer in lvset and make client call only if the value changed
	changed := common.SetFinalizer(&lvSet.ObjectMeta, common.LocalVolumeProtectionFinalizer, setFinalizer)
	if changed {
		return r.client.Update(context.TODO(), &lvSet)
	}

	return nil

}
