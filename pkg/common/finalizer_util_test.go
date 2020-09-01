package common

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// some const strings to use as vars
	stringA = "a"
	stringB = "b"
	stringC = "c"
	stringD = "d"
)

func TestContainsFinalizer(t *testing.T) {

	testTable := []struct {
		// args
		metadata  metav1.ObjectMeta
		finalizer string
		// results to assert
		shouldExist bool
	}{
		{
			metadata: metav1.ObjectMeta{
				Finalizers: []string{stringA},
			},
			finalizer:   stringA,
			shouldExist: true,
		},
		{
			metadata: metav1.ObjectMeta{
				Finalizers: []string{stringA, stringB, stringC},
			},
			finalizer:   stringA,
			shouldExist: true,
		},
		{
			metadata: metav1.ObjectMeta{
				Finalizers: []string{stringA},
			},
			finalizer:   stringB,
			shouldExist: false,
		},
		{
			metadata: metav1.ObjectMeta{
				Finalizers: []string{stringA, stringB, stringC},
			},
			finalizer:   stringD,
			shouldExist: false,
		},
		{
			metadata: metav1.ObjectMeta{
				Finalizers: []string{},
			},
			finalizer:   stringD,
			shouldExist: false,
		},
	}

	for _, testCase := range testTable {
		t.Logf("testCase: %+v", testCase)

		exists := ContainsFinalizer(testCase.metadata, testCase.finalizer)
		assert.Equal(t, testCase.shouldExist, exists)
	}
}

func TestSetFinalizer(t *testing.T) {
	testTable := []struct {
		// args
		metadata     *metav1.ObjectMeta
		finalizer    string
		setFinalizer bool
		// results to assert
		beforeSet          []string
		expectedFinalizers []string
		shouldChange       bool
	}{
		// add finalizer, should not change
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA}},
			finalizer:          stringA,
			setFinalizer:       true,
			expectedFinalizers: []string{stringA},
			shouldChange:       false,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA, stringB}},
			finalizer:          stringA,
			setFinalizer:       true,
			expectedFinalizers: []string{stringA, stringB},
			shouldChange:       false,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringB, stringA, stringC}},
			finalizer:          stringA,
			setFinalizer:       true,
			expectedFinalizers: []string{stringB, stringA, stringC},
			shouldChange:       false,
		},
		// add finalalizer, should change
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{}},
			finalizer:          stringC,
			setFinalizer:       true,
			expectedFinalizers: []string{stringC},
			shouldChange:       true,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA, stringB}},
			finalizer:          stringC,
			setFinalizer:       true,
			expectedFinalizers: []string{stringA, stringB, stringC},
			shouldChange:       true,
		},
		// remove finalizer , should not change
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{}},
			finalizer:          stringC,
			setFinalizer:       false,
			expectedFinalizers: []string{},
			shouldChange:       false,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA}},
			finalizer:          stringD,
			setFinalizer:       false,
			expectedFinalizers: []string{stringA},
			shouldChange:       false,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA, stringB}},
			finalizer:          stringD,
			setFinalizer:       false,
			expectedFinalizers: []string{stringA, stringB},
			shouldChange:       false,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringB, stringA, stringC}},
			finalizer:          stringD,
			setFinalizer:       false,
			expectedFinalizers: []string{stringB, stringA, stringC},
			shouldChange:       false,
		},
		// remove finalizer , should change
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringC}},
			finalizer:          stringC,
			setFinalizer:       false,
			expectedFinalizers: []string{},
			shouldChange:       true,
		},
		{
			metadata:           &metav1.ObjectMeta{Finalizers: []string{stringA, stringB}},
			finalizer:          stringA,
			setFinalizer:       false,
			expectedFinalizers: []string{stringB},
			shouldChange:       true,
		},
	}

	for _, testCase := range testTable {
		t.Logf("testCase: %+v", testCase)

		oldFinalizers := append([]string{}, testCase.metadata.Finalizers...)
		changed := SetFinalizer(testCase.metadata, testCase.finalizer, testCase.setFinalizer)
		assert.Equal(t, testCase.shouldChange, changed)

		changedFinalizers := testCase.metadata.Finalizers
		changedFinalizerSet := sets.NewString(changedFinalizers...)
		expectedFinalizerSet := sets.NewString(testCase.expectedFinalizers...)

		assert.Truef(t, changedFinalizerSet.Equal(expectedFinalizerSet), "expect changed and expected finalizer set to be equal")

		//expect nothing to change unless `changed` is true
		expectedLen := 0
		if changed {
			expectedLen = 1
		}
		assert.Equalf(t, int(math.Abs(float64(len(changedFinalizers)-len(oldFinalizers)))), expectedLen, "changed not accurate")

		// assert expected state of finalizers
		assert.True(t, expectedFinalizerSet.Equal(changedFinalizerSet))
	}
}
