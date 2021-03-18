package lv

import (
	"github.com/openshift/local-storage-operator/pkg/apis"
	localv1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/mount"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	//event "github.com/openshift/local-storage-operator/pkg/diskmaker"

	// sig-local-static-provisioner libs
	provCache "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/cache"
	provCommon "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/common"

	provDeleter "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/deleter"
	provUtil "sigs.k8s.io/sig-storage-local-static-provisioner/pkg/util"
)

const (
	// ComponentName for lv symlinker
	ComponentName = "localvolume-symlink-controller"
)

var log = logf.Log.WithName(ComponentName)

// Add adds a new nodeside lv controller to mgr
func Add(mgr manager.Manager, cleanupTracker provDeleter.CleanupStatusTracker, pvCache *provCache.VolumeCache) error {
	apis.AddToScheme(mgr.GetScheme())
	// populate the pv cache
	clientSet := provCommon.SetupClient()
	runtimeConfig := &provCommon.RuntimeConfig{
		UserConfig: &provCommon.UserConfig{
			Node: &corev1.Node{},
		},
		Cache:    provCache.NewVolumeCache(),
		VolUtil:  provUtil.NewVolumeUtil(),
		APIUtil:  provUtil.NewAPIUtil(clientSet),
		Client:   clientSet,
		Recorder: mgr.GetEventRecorderFor(ComponentName),
		Mounter:  mount.New("" /* defaults to /bin/mount */),
		// InformerFactory: , // unused

	}

	// TODO:run one for both lv and lvset
	cleanupTracker := &provDeleter.CleanupStatusTracker{ProcTable: provDeleter.NewProcTable()}
	// TODO: populate the cache or share one with lvset

	r := &ReconcileLocalVolume{
		client:    mgr.GetClient(),
		scheme:    mgr.GetScheme(),
		eventSync: newEventReporter(mgr.GetEventRecorderFor(ComponentName)),
		// TODO: switch with the getter method
		symlinkLocation: "/mnt/local-storage",
		cleanupTracker:  cleanupTracker,
		runtimeConfig:   runtimeConfig,
		deleter:         provDeleter.NewDeleter(runtimeConfig, cleanupTracker),
	}
	// Create a new controller
	//	apis.AddToScheme(r.scheme)
	c, err := controller.New(ComponentName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	err = c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{
		OwnerType: &localv1.LocalVolume{},
	})

	err = c.Watch(&source.Kind{Type: &localv1.LocalVolume{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

type ReconcileLocalVolume struct {
	client          client.Client
	scheme          *runtime.Scheme
	symlinkLocation string
	localVolume     *localv1.LocalVolume
	eventSync       *eventReporter

	// static-provisioner stuff
	cleanupTracker *provDeleter.CleanupStatusTracker
	runtimeConfig  *provCommon.RuntimeConfig
	deleter        *provDeleter.Deleter
	firstRunOver   bool
}

var _ reconcile.Reconciler = &ReconcileLocalVolume{}
