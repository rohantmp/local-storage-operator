package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	framework "github.com/operator-framework/operator-sdk/pkg/test"

	localv1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1"
	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	"github.com/openshift/local-storage-operator/pkg/common"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	labelNodeRoleWorker = "node-role.kubernetes.io/worker"
	pvConsumerLabel     = "pv-consumer"
)

func LocalVolumeSetTest(ctx *framework.Context, cleanupFuncs *[]cleanupFn) func(*testing.T) {
	return func(t *testing.T) {

		f := framework.Global
		namespace, err := ctx.GetNamespace()
		if err != nil {
			t.Fatalf("error fetching namespace : %v", err)
		}

		matcher := gomega.NewGomegaWithT(t)
		gomega.SetDefaultEventuallyTimeout(time.Minute * 10)
		gomega.SetDefaultEventuallyPollingInterval(time.Second * 2)

		// get nodes
		nodeList := &corev1.NodeList{}
		err = f.Client.List(context.TODO(), nodeList, client.HasLabels{labelNodeRoleWorker})
		if err != nil {
			t.Fatalf("failed to list nodes: %+v", err)
		}

		minNodes := 3
		if len(nodeList.Items) < minNodes {
			t.Fatalf("expected to have at least %d nodes", minNodes)
		}

		// represents the disk layout to setup on the nodes.
		nodeEnv := []nodeDisks{
			{
				disks: []int{10, 20, 30, 40, 70},
				node:  nodeList.Items[0],
			},
			{
				disks: []int{10, 20, 30, 40, 70},
				node:  nodeList.Items[1],
			},
			{
				disks: []int{10, 20, 30, 40, 70},
				node:  nodeList.Items[2],
			},
		}

		t.Log("getting AWS region info from node spec")
		_, region, _, err := getAWSNodeInfo(nodeList.Items[0])
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "getAWSNodeInfo")

		// initialize client
		t.Log("initialize ec2 creds")
		ec2Client, err := getEC2Client(region)
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "getEC2Client")

		// cleanup host dirs

		addToCleanupFuncs(cleanupFuncs, "cleanupSymlinkDir", func(t *testing.T) error {
			return cleanupSymlinkDir(t, ctx, nodeEnv)
		})
		// register disk cleanup
		addToCleanupFuncs(cleanupFuncs, "cleanupAWSDisks", func(t *testing.T) error {
			return cleanupAWSDisks(t, ec2Client)
		})

		// create and attach volumes
		t.Log("creating and attaching disks")
		for _, nodeDisks := range nodeEnv {
			_, err := createAndAttachAWSVolumes(t, ec2Client, ctx, namespace, nodeDisks.node, nodeDisks.disks...)
			matcher.Expect(err).NotTo(gomega.HaveOccurred(), "createAndAttachAWSVolumes: %+v", nodeDisks)
		}
		tenGi := resource.MustParse("10G")
		twentyGi := resource.MustParse("20G")
		thirtyGi := resource.MustParse("30G")
		fiftyGi := resource.MustParse("50G")
		two := int32(2)
		three := int32(3)

		lvSets := []*localv1alpha1.LocalVolumeSet{}

		// start the lvset with a size range of twenty to fifty on the first node
		// should match amd claim 2 of node0: 20, 30, 40
		// total: 2
		// disks  left within range:
		// node0: 2
		// node1: 3
		// node2: 3
		twentyToFifty := &localv1alpha1.LocalVolumeSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "twentytofifty-1",
				Namespace: namespace,
			},
			Spec: localv1alpha1.LocalVolumeSetSpec{
				StorageClassName: "twentytofifty-1",
				MaxDeviceCount:   &two,
				VolumeMode:       localv1.PersistentVolumeBlock,
				NodeSelector: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{nodeEnv[0].node.ObjectMeta.Labels[corev1.LabelHostname]},
							},
						},
					},
				},
				},
				DeviceInclusionSpec: &localv1alpha1.DeviceInclusionSpec{
					DeviceTypes: []localv1alpha1.DeviceType{localv1alpha1.RawDisk},
					MinSize:     &twentyGi,
					MaxSize:     &fiftyGi,
				},
			},
		}
		lvSets = append(lvSets, twentyToFifty)

		// create an identical lvset with an overlap
		// should match amd claim 1 of node0: 10
		// total: 1
		// disks  left within range left:
		// node0: 1
		// node1: 4
		// node2: 4
		tenToThirty := &localv1alpha1.LocalVolumeSet{}
		twentyToFifty.DeepCopyInto(tenToThirty)
		tenToThirty.ObjectMeta.Name = fmt.Sprintf("tentothirty-overlapping-%s-2", twentyToFifty.GetName())
		tenToThirty.Spec.StorageClassName = tenToThirty.GetName()

		// introduce differences
		tenToThirty.Spec.DeviceInclusionSpec.MinSize = &tenGi
		tenToThirty.Spec.DeviceInclusionSpec.MaxSize = &thirtyGi

		// filesystem lvset that only matches the 3rd node
		twentyToFiftyFilesystem := &localv1alpha1.LocalVolumeSet{}
		twentyToFifty.DeepCopyInto(twentyToFiftyFilesystem)
		twentyToFiftyFilesystem.ObjectMeta.Name = "twentytofifty-fs-3"
		twentyToFiftyFilesystem.Spec.StorageClassName = twentyToFiftyFilesystem.GetName()
		twentyToFiftyFilesystem.Spec.NodeSelector.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = nodeEnv[2].node.ObjectMeta.Labels[corev1.LabelHostname]

		twentyToFiftyFilesystem.Spec.VolumeMode = localv1.PersistentVolumeFilesystem

		lvSets = []*localv1alpha1.LocalVolumeSet{
			twentyToFifty,
			tenToThirty,
			twentyToFiftyFilesystem,
		}

		// add pv and storageclass cleanup
		addToCleanupFuncs(
			cleanupFuncs,
			"cleanupLVSetResources",
			func(t *testing.T) error {
				return cleanupLVSetResources(t, lvSets)
			},
		)

		t.Logf("creating localvolumeset %q", twentyToFifty.GetName())
		err = f.Client.Create(context.TODO(), twentyToFifty, &framework.CleanupOptions{TestContext: ctx})
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "create localvolumeset")

		// look for 2 PVs
		eventuallyFindPVs(t, f, twentyToFifty.Spec.StorageClassName, 2)

		// update lvset
		// already claimed: 2
		// should match amd claim 1 of node0: 20,30,40
		// total: 3
		// disks  left within range:
		// node0: 0
		// node1: 3
		// node2: 3
		matcher.Eventually(func() error {
			t.Log("updating lvset")
			key := types.NamespacedName{Name: twentyToFifty.GetName(), Namespace: twentyToFifty.GetNamespace()}
			err := f.Client.Get(context.TODO(), key, twentyToFifty)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}

			twentyToFifty.Spec.MaxDeviceCount = &three
			err = f.Client.Update(context.TODO(), twentyToFifty)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}
			return nil
		}, time.Minute, time.Second*2).ShouldNot(gomega.HaveOccurred(), "updating lvset")

		// look for 3 PVs
		eventuallyFindPVs(t, f, twentyToFifty.Spec.StorageClassName, 3)

		t.Logf("creating localvolumeset %q", tenToThirty.GetName())
		err = f.Client.Create(context.TODO(), tenToThirty, &framework.CleanupOptions{TestContext: ctx})
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "create localvolumeset")

		// look for 1 PV
		eventuallyFindPVs(t, f, tenToThirty.Spec.StorageClassName, 1)

		// expand overlappingLVSet to node1
		// already claimed: 1
		// should match amd claim 2 of node1: 10, 20, 30
		// total: 3
		// disks  left within range:
		// node0: 0
		// node1: 2
		// node2: 3
		matcher.Eventually(func() error {
			t.Log("updating lvset")
			key := types.NamespacedName{Name: tenToThirty.GetName(), Namespace: tenToThirty.GetNamespace()}
			err := f.Client.Get(context.TODO(), key, tenToThirty)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}

			// update node selector
			tenToThirty.Spec.NodeSelector.NodeSelectorTerms[0].MatchExpressions[0].Values = append(
				tenToThirty.Spec.NodeSelector.NodeSelectorTerms[0].MatchExpressions[0].Values,
				nodeEnv[1].node.ObjectMeta.Labels[corev1.LabelHostname],
			)
			err = f.Client.Update(context.TODO(), tenToThirty)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}
			return nil
		}, time.Minute, time.Second*2).ShouldNot(gomega.HaveOccurred(), "updating lvset")

		// look for 3 PVs
		eventuallyFindPVs(t, f, tenToThirty.Spec.StorageClassName, 3)

		// create twentyToFiftyFilesystem
		// should match amd claim 2 of node2: 20, 30, 40
		// total: 2
		// disks  left within range:
		// node0: 0
		// node1: 1
		// node2: 1
		t.Logf("creating localvolumeset %q", twentyToFiftyFilesystem.GetName())
		err = f.Client.Create(context.TODO(), twentyToFiftyFilesystem, &framework.CleanupOptions{TestContext: ctx})
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "create localvolumeset")

		eventuallyFindPVs(t, f, twentyToFiftyFilesystem.Spec.StorageClassName, 2)

		// expand twentyToFiftyFilesystem to all remaining devices by setting nil nodeSelector, maxDevices, and deviceInclusionSpec
		// devices total: 15
		// devices claimed so far by other lvsets: 6
		// devices claimed so far: 2
		// new devices to be claimed: (15-(6+2))= 7
		// total: 7+2 = 9

		matcher.Eventually(func() error {
			t.Log("updating lvset")
			key := types.NamespacedName{Name: twentyToFiftyFilesystem.GetName(), Namespace: twentyToFiftyFilesystem.GetNamespace()}
			err := f.Client.Get(context.TODO(), key, twentyToFiftyFilesystem)
			if err != nil {
				t.Logf("error getting lvset %q, %+v", key, err)
				return err
			}

			// update node selector
			twentyToFiftyFilesystem.Spec.NodeSelector = nil

			// update maxDeviceCount
			twentyToFiftyFilesystem.Spec.MaxDeviceCount = nil

			// update deviceInclusionSpec
			twentyToFiftyFilesystem.Spec.DeviceInclusionSpec = nil

			err = f.Client.Update(context.TODO(), twentyToFiftyFilesystem)
			if err != nil {
				t.Logf("error updating lvset %q: %+v", key, err)
				return err
			}
			return nil
		}, time.Minute, time.Second*2).ShouldNot(gomega.HaveOccurred(), "updating lvset")
		pvList := eventuallyFindPVs(t, f, twentyToFiftyFilesystem.Spec.StorageClassName, 9)
		_ = pvList
		// verifyReclaimPolicyDelete

		verifyRecliamPolicyDelete(t, f, ctx, twentyToFiftyFilesystem.Spec.StorageClassName, pvList)

	}

}

func verifyRecliamPolicyDelete(t *testing.T, f *framework.Framework, ctx *framework.Context, storageClassName string, pvList []corev1.PersistentVolume) {
	matcher := gomega.NewGomegaWithT(t)

	jobList := make([]batchv1.Job, 0)
	for _, pv := range pvList {
		job, _ := claimAndReleasePV(t, f, ctx, pv)
		jobList = append(jobList, job)
	}
	matcher.Eventually(func() bool {
		var err error
		for _, existingJobs := range jobList {
			job := &batchv1.Job{}
			err = f.Client.Get(context.TODO(), types.NamespacedName{Name: existingJobs.Name, Namespace: existingJobs.Namespace}, job)
			if err != nil {
				t.Logf("failed to get job: %q", existingJobs.Name)
				return false
			}
			if job.Status.CompletionTime != nil {
				return false
			} else {
				eventuallyDelete(t, job, job.Name)
			}
		}
		return true
	}, time.Minute*2, time.Second*2).Should(gomega.BeTrue(), "waiting for all pv-consumer jobs to be completed")

	matcher.Eventually(func() error {
		job := &batchv1.Job{}
		err := f.Client.DeleteAllOf(context.TODO(), job, &client.MatchingLabels{"app": pvConsumerLabel})
		return err
	}).ShouldNot(gomega.HaveOccurred(), "deleting pv-consumer jobs")

	var foundCount int
	matcher.Eventually(func() bool {
		var err error
		foundCount = 0
		for _, existingPV := range pvList {
			pv := &corev1.PersistentVolume{}
			err = f.Client.Get(context.TODO(), types.NamespacedName{Name: existingPV.Name, Namespace: existingPV.Namespace}, pv)
			if errors.IsNotFound(err) {
				continue
			} else if err != nil {
				t.Logf("failed to get pv: %q", pv.Name)
				continue
			}
			if existingPV.UID == pv.UID {
				// the PV has not been deleted yet
				continue
			}
			foundCount++
		}
		if foundCount == len(pvList) {
			return true
		}
		return false
	}, time.Minute*3, time.Second*2).Should(gomega.BeTrue(), "waiting for all the pvs to be recreated")

	// list n
	// consume n PVs
	// wait for Bound Phase
	// write something
	// release
	// verify uuid deletion
	// verify PV is replaced with same name
	// attempt to read something?

}

func claimAndReleasePV(t *testing.T, f *framework.Framework, ctx *framework.Context, pv corev1.PersistentVolume) (batchv1.Job, corev1.PersistentVolumeClaim) {
	claimName := fmt.Sprintf("%s-consumer", pv.Name)
	capacity, found := pv.Spec.Capacity[corev1.ResourceStorage]
	if !found {
		t.Fatalf("storage capacity not found in pv: %q", pv.Name)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: f.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &pv.Spec.StorageClassName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:       pv.Spec.VolumeMode,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: capacity,
				},
			},
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: f.Namespace,
			Labels: map[string]string{
				"app": pvConsumerLabel,
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:  "data-writer",
							Image: common.GetDiskMakerImage(),
							VolumeMounts: []corev1.VolumeMount{
								{
									MountPath: "/data",
									Name:      "volume-to-debug",
								},
							},
							Command: []string{"/bin/sh", "-c"},
							Args: []string{
								"set -x",
								"dd if=/dev/zero of=/tmp/random.img bs=512 count=1",       // create a new file named random.img
								"md5VAR1=$(md5sum /tmp/random.img | awk '{ print $1 }')",  // get md5 hash
								"cp /tmp/random.img /data/random.img",                     // copy random.img file to pvc mountpoint
								"md5VAR2=$(md5sum /data/random.img | awk '{ print $1 }')", // calculate md5sum in new location
								"if [[ \"$md5VAR1\" != \"$md5VAR2\" ]];then exit 1; fi;",  // compare the two
								"sync;", // ensure write
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "volume-to-debug",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: claimName,
								},
							},
						},
					},
				},
			},
		},
	}

	started := metav1.NewTime(time.Now())
	matcher := gomega.NewGomegaWithT(t)
	matcher.Eventually(func() error {
		t.Logf("creating job and pvc to consume pv %q with volumeMode %q", pv.Name, *pv.Spec.VolumeMode)
		// create pvc
		err := f.Client.Create(context.TODO(), pvc, &framework.CleanupOptions{TestContext: ctx})
		if errors.IsAlreadyExists(err) {
			// get existing
			err = f.Client.Get(
				context.TODO(),
				types.NamespacedName{Name: pvc.GetName(), Namespace: pvc.GetNamespace()},
				job,
			)
			if err != nil {
				return err
			}
			// delete it if it stale one exists
			if job.CreationTimestamp.Before(&started) {
				eventuallyDelete(t, pvc, fmt.Sprintf("existing %v", pvc.GetName()))
				// restart
				return fmt.Errorf("stale pvc exists")
			}
		} else if err != nil {
			return err
		}

		// create job
		err = f.Client.Create(context.TODO(), job, &framework.CleanupOptions{TestContext: ctx})
		if errors.IsAlreadyExists(err) {
			// get existing
			err = f.Client.Get(
				context.TODO(),
				types.NamespacedName{Name: job.GetName(), Namespace: job.GetNamespace()},
				job,
			)
			if err != nil {
				return err
			}
			// delete it if it stale one exists exists
			if job.CreationTimestamp.Before(&started) {
				eventuallyDelete(t, job, fmt.Sprintf("existing %v", job.GetName()))
				// restart
				return fmt.Errorf("stale job existes")
			}
		}
		return err
	}).ShouldNot(gomega.HaveOccurred(), "creating job and pv")
	return *job, *pvc
}

func cleanupLVSetResources(t *testing.T, lvsets []*localv1alpha1.LocalVolumeSet) error {
	for _, lvset := range lvsets {

		t.Logf("cleaning up pvs and storageclasses: %q", lvset.GetName())
		f := framework.Global
		matcher := gomega.NewWithT(t)
		sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: lvset.Spec.StorageClassName}}

		eventuallyDelete(t, lvset, lvset.GetName())
		eventuallyDelete(t, sc, sc.GetName())
		pvList := &corev1.PersistentVolumeList{}
		t.Logf("listing pvs for lvset: %q", lvset.GetName())
		matcher.Eventually(func() error {
			err := f.Client.List(context.TODO(), pvList)
			if err != nil {
				return err
			}
			t.Logf("Deleting %d PVs", len(pvList.Items))
			for _, pv := range pvList.Items {
				if pv.Spec.StorageClassName == lvset.Spec.StorageClassName {
					eventuallyDelete(t, &pv, pv.GetName())
				}
			}
			return nil
		}, time.Minute*3, time.Second*2).ShouldNot(gomega.HaveOccurred(), "cleaning up pvs for lvset: %q", lvset.GetName())
	}

	return nil
}
