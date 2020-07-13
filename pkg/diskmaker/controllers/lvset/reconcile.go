package lvset

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	"github.com/openshift/local-storage-operator/pkg/controller/nodedaemon"
	"github.com/openshift/local-storage-operator/pkg/internal"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	staticProvisioner "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"
)

// Reconcile reads that state of the cluster for a LocalVolumeSet object and makes changes based on the state read
// and what is in the LocalVolumeSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileLocalVolumeSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling LocalVolumeSet")

	// Fetch the LocalVolumeSet instance
	instance := &localv1alpha1.LocalVolumeSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// get node name
	nodeName := getNodeNameEnvVar()
	// get node labels
	node := &corev1.Node{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: nodeName}, node)
	if err != nil {
		return reconcile.Result{}, err
	}

	// ignore LocalVolmeSets whose LabelSelector doesn't match this node
	// NodeSelectorTerms.MatchExpressions are ORed
	matches, err := nodeSelectorMatchesNodeLabels(node, instance.Spec.NodeSelector)
	if err != nil {
		reqLogger.Error(err, "failed to match nodeSelector to node labels")
		return reconcile.Result{}, err
	}

	if !matches {
		return reconcile.Result{}, nil
	}

	storageClass := instance.Spec.StorageClassName

	// get associated config
	cm := &corev1.ConfigMap{}
	// TOODO(rohan) export/import name
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: nodedaemon.ProvisionerConfigMapName, Namespace: request.Namespace}, cm)
	if err != nil {
		reqLogger.Error(err, "could not get provisioner configmap")
		return reconcile.Result{}, err
	}

	provisionerConfig := staticProvisioner.ProvisionerConfiguration{}
	staticProvisioner.ConfigMapDataToVolumeConfig(cm.Data, &provisionerConfig)
	// find disks that match lvset filter

	symLinkConfig, ok := provisionerConfig.StorageClassConfig[storageClass]
	if !ok {
		return reconcile.Result{}, fmt.Errorf("could not find storageclass entry %q in provisioner config: %+v", storageClass, provisionerConfig)
	}
	symLinkDir := symLinkConfig.HostDir

	blockDevices, badRows, err := internal.ListBlockDevices()
	if err != nil {
		reqLogger.Error(err, "could not list block devices", "lsblk.BadRows", badRows)
		return reconcile.Result{}, err
	} else if len(badRows) > 0 {
		reqLogger.Error(fmt.Errorf("bad rows"), "could not parse all the lsblk rows", "lsblk.BadRows", badRows)
	}

	validDevices := make([]internal.BlockDevice, 0)

	// get valid devices
DeviceLoop:
	for _, blockDevice := range blockDevices {
		devLogger := reqLogger.WithValues("Device.Name", blockDevice.Name)
		for name, filter := range filterMap {
			filterLogger := devLogger.WithValues("filter.Name", name)
			valid, err := filter(blockDevice, *instance.Spec.DeviceInclusionSpec)
			if err != nil {
				filterLogger.Error(err, "filter error")
				valid = false
				continue DeviceLoop
			} else if !valid {
				filterLogger.Info("filter negative")
				continue DeviceLoop
			}
		}
		for name, matcher := range matcherMap {
			matcherLogger := devLogger.WithValues("matcher.Name", name)
			valid, err := matcher(blockDevice, *instance.Spec.DeviceInclusionSpec)
			if err != nil {
				matcherLogger.Error(err, "match error")
				valid = false
				continue DeviceLoop
			} else if !valid {
				matcherLogger.Info("match negative")
				continue DeviceLoop
			}
		}
		devLogger.Info("matched disk, symlinking")
		// handle valid disk
		validDevices = append(validDevices, blockDevice)

	}

	// process files in symlinkdir:
	// matches PV, delete PV
	// is Symlink, matching symlink, leave it alone, else remove it
	// remove finalizer.

	// list PVs

	// pvList := &corev1.PersistentVolumeList{}
	// err = r.client.List(
	// 	context.TODO(),
	// 	pvList,
	// 	client.MatchingLabels{
	// 		util.OwnerNameLabel:      instance.GetName(),
	// 		util.OwnerNamespaceLabel: instance.GetNamespace(),
	// 	},
	// )

	// process valid devices
	var noMatch []string
	for _, blockDevice := range validDevices {
		var alreadyProvisionedCount int
		alreadyProvisionedCount, noMatch, err = getAlreadyProvisioned(symLinkDir, validDevices)
		if err != nil {
			return reconcile.Result{}, errors.Wrap(err, "could not determine how many devices are already provisioned")
		}
		devLogger := reqLogger.WithValues("Device.Name", blockDevice.Name)
		withinMax := true
		if instance.Spec.MaxDeviceCount != nil {
			withinMax = int32(alreadyProvisionedCount) < *instance.Spec.MaxDeviceCount
		}
		if !withinMax {
			devLogger.Info("not symlinking, max device count exceeded")
			break
		}
		devLogger.Info("symlinking")
		err = symLinkDisk(blockDevice, symLinkDir)
		if err != nil {
			return reconcile.Result{}, errors.Wrap(err, "could not symlink disk")
		}
		devLogger.Info("symlinking succeeded")

	}
	if len(noMatch) > 0 {
		reqLogger.Info("found stale symLink Entries", "storageClass.Name", storageClass, "paths.List", noMatch)
	}

	// process devices to clean up

	return reconcile.Result{Requeue: true, RequeueAfter: time.Minute}, nil
}

func getAlreadyProvisioned(symLinkDir string, validDevices []internal.BlockDevice) (int, []string, error) {
	count := 0
	noMatch := make([]string, 0)
	paths, err := filepath.Glob(filepath.Join(symLinkDir, "/*"))
	if err != nil {
		return 0, []string{}, err
	}

PathLoop:
	for _, path := range paths {
		for _, device := range validDevices {
			isMatch, err := internal.PathEvalsToDiskLabel(path, device.Name)
			if err != nil {
				return 0, []string{}, err
			}
			if isMatch {
				count++
				continue PathLoop
			}
		}
		noMatch = append(noMatch, path)
	}
	return count, noMatch, nil
}

func symLinkDisk(dev internal.BlockDevice, symLinkDir string) error {
	pathByID, err := dev.GetPathByID()
	// TODO: handle IDPathNotFoundError (/dev/label path)?
	if err != nil {
		return err
	}
	// ensure symLinkDirExists
	err = os.MkdirAll(symLinkDir, 0755)
	if err != nil {
		return fmt.Errorf("could not create symlinkdir: %w", err)
	}
	deviceID := filepath.Base(pathByID)
	symLinkPath := path.Join(symLinkDir, deviceID)

	// create symlink
	err = os.Symlink(pathByID, symLinkPath)
	if os.IsExist(err) {
		fileInfo, statErr := os.Stat(pathByID)
		if statErr != nil {
			return fmt.Errorf("could not create symlink: %v,%w", err, statErr)

			// existing file is symlink
		} else if fileInfo.Mode() == os.ModeSymlink {
			valid, evalErr := internal.PathEvalsToDiskLabel(pathByID, dev.Name)
			if evalErr != nil {
				return fmt.Errorf("existing symlink not valid: %v,%w", err, evalErr)
				// existing file evals to disk
			} else if valid {
				// if file exists and is accurate symlink, skip and return success
				return nil
			}
		}
	} else if err != nil {
		return err
	}

	return nil
}

func nodeSelectorMatchesNodeLabels(node *corev1.Node, nodeSelector *corev1.NodeSelector) (bool, error) {
	if nodeSelector == nil {
		return false, fmt.Errorf("the nodeSelector var is nil")
	}
	if node == nil {
		return false, fmt.Errorf("the node var is nil")
	}
	matches := corev1helper.MatchNodeSelectorTerms(nodeSelector.NodeSelectorTerms, node.Labels, fields.Set{
		"metadata.name": node.Name,
	})
	return matches, nil
}

func getNodeNameEnvVar() string {
	return os.Getenv("MY_NODE_NAME")
}
