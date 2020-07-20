package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	framework "github.com/operator-framework/operator-sdk/pkg/test"

	localv1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1"
	localv1alpha1 "github.com/openshift/local-storage-operator/pkg/apis/local/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	labelNodeRoleWorker = "node-role.kubernetes.io/worker"
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
				disks: []int{10, 20, 30, 40},
				node:  nodeList.Items[0],
			},
			{
				disks: []int{10, 20, 30, 40},
				node:  nodeList.Items[1],
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
		fiftyGi := resource.MustParse("50G")
		two := int32(2)
		three := int32(3)

		lvSets := []*localv1alpha1.LocalVolumeSet{}

		// start the lvset with a size range of twenty to fifty on the first node
		// will be created first
		// should match amd claim nodeEnv[0].disks: 20,30,40
		smallLVSet := &localv1alpha1.LocalVolumeSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "small-1",
				Namespace: namespace,
			},
			Spec: localv1alpha1.LocalVolumeSetSpec{
				StorageClassName: "small-1",
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
		lvSets = append(lvSets, smallLVSet)

		// create an identical lvset with an overlap
		// nodeEnv[0].disks
		// match and claim 1 disk from: 10
		// match and not claim 3 disks from: 20, 30, 40
		overlappingLVSet := &localv1alpha1.LocalVolumeSet{}
		smallLVSet.DeepCopyInto(overlappingLVSet)
		overlappingLVSet.ObjectMeta.Name = fmt.Sprintf("overlapping-%s", smallLVSet.GetName())
		overlappingLVSet.Spec.StorageClassName = overlappingLVSet.GetName()

		// introduce differences
		overlappingLVSet.Spec.DeviceInclusionSpec.MinSize = &tenGi

		lvSets = []*localv1alpha1.LocalVolumeSet{
			smallLVSet,
			overlappingLVSet,
		}

		// add pv and storageclass cleanup
		addToCleanupFuncs(
			cleanupFuncs,
			"cleanupLVSetResources",
			func(t *testing.T) error {
				return cleanupLVSetResources(t, lvSets)
			},
		)

		t.Logf("creating localvolumeset %q", smallLVSet.GetName())
		err = f.Client.Create(context.TODO(), smallLVSet, &framework.CleanupOptions{TestContext: ctx})
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "create localvolumeset")

		// look for 2 PVs
		eventuallyFindPVs(t, f, smallLVSet.Spec.StorageClassName, 2)

		// update lvset
		matcher.Eventually(func() error {
			t.Log("updating lvset")
			key := types.NamespacedName{Name: smallLVSet.GetName(), Namespace: smallLVSet.GetNamespace()}
			err := f.Client.Get(context.TODO(), key, smallLVSet)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}

			smallLVSet.Spec.MaxDeviceCount = &three
			err = f.Client.Update(context.TODO(), smallLVSet)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}
			return nil
		}, time.Minute, time.Second*2).ShouldNot(gomega.HaveOccurred(), "updating lvset")

		// look for 3 PVs
		eventuallyFindPVs(t, f, smallLVSet.Spec.StorageClassName, 3)

		t.Logf("creating localvolumeset %q", overlappingLVSet.GetName())
		err = f.Client.Create(context.TODO(), overlappingLVSet, &framework.CleanupOptions{TestContext: ctx})
		matcher.Expect(err).NotTo(gomega.HaveOccurred(), "create localvolumeset")

		// look for 1 PV
		eventuallyFindPVs(t, f, overlappingLVSet.Spec.StorageClassName, 1)

		// update overlapping lvset to match 2nd node
		// nodeEnv[0].disks
		// match and claim 1 disk from: 10
		// nodeEnv[0].disks
		// match and  claim 2 disks from: 10, 20, 30, 40
		matcher.Eventually(func() error {
			t.Log("updating lvset")
			key := types.NamespacedName{Name: overlappingLVSet.GetName(), Namespace: overlappingLVSet.GetNamespace()}
			err := f.Client.Get(context.TODO(), key, overlappingLVSet)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}

			// update node selector
			overlappingLVSet.Spec.NodeSelector.NodeSelectorTerms[0].MatchExpressions[0].Values = append(
				overlappingLVSet.Spec.NodeSelector.NodeSelectorTerms[0].MatchExpressions[0].Values,
				nodeEnv[1].node.ObjectMeta.Labels[corev1.LabelHostname],
			)
			err = f.Client.Update(context.TODO(), overlappingLVSet)
			if err != nil {
				t.Logf("error getting lvset %q: %+v", key, err)
				return err
			}
			return nil
		}, time.Minute, time.Second*2).ShouldNot(gomega.HaveOccurred(), "updating lvset")

		// look for 3 PVs
		eventuallyFindPVs(t, f, overlappingLVSet.Spec.StorageClassName, 3)

	}

}

// map from node to distribution of disks on that node.
// when executing, a node from the list of available nodes will be assigned one of these configurations and keys
// behaviour unverified for more than 15 disks per node.
// it

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
