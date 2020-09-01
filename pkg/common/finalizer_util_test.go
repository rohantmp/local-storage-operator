package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
