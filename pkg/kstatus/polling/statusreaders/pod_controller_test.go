// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package statusreaders

import (
	"context"
	"fmt"
	"testing"
	"time"

	fakecr "github.com/fluxcd/cli-utils/pkg/kstatus/polling/clusterreader/fake"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	fakemapper "github.com/fluxcd/cli-utils/pkg/testutil"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestPodControllerStatusReader(t *testing.T) {
	rsGVK = v1.SchemeGroupVersion.WithKind("ReplicaSet")
	name := "Foo"
	namespace := "Bar"

	testCases := map[string]struct {
		computeStatusResult *status.Result
		computeStatusErr    error
		genResourceStatuses event.ResourceStatuses
		expectedIdentifier  object.ObjMetadata
		expectedStatus      status.Status
	}{
		"successfully computes status": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.InProgressStatus,
		},
		"computing status fails": {
			computeStatusErr: fmt.Errorf("this error is a test"),
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.UnknownStatus,
		},
		"one of the pods has failed status": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status: status.CurrentStatus,
				},
				{
					Status: status.InProgressStatus,
				},
				{
					Status:   status.FailedStatus,
					Resource: newPodWithPhase("failed-pod", namespace, corev1.PodFailed),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.FailedStatus,
		},
		"pending unschedulable pod does not cause failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: newUnschedulablePod("pending-pod", namespace),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.InProgressStatus,
		},
		"pending pod without unschedulable condition causes failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: newPodWithPhase("pending-pod", namespace, corev1.PodPending),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.FailedStatus,
		},
		"deleted pod does not cause failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: newDeletingPod("deleting-pod", namespace),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.InProgressStatus,
		},
		"mix of transient and terminal pod failures causes failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: newUnschedulablePod("pending-pod", namespace),
				},
				{
					Status:   status.FailedStatus,
					Resource: newPodWithPhase("crashed-pod", namespace, corev1.PodFailed),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.FailedStatus,
		},
		"failed pod with nil resource does not cause failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: nil,
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.InProgressStatus,
		},
		"running pod with crash loop causes failure": {
			computeStatusResult: &status.Result{
				Status:  status.InProgressStatus,
				Message: "this is a test",
			},
			genResourceStatuses: event.ResourceStatuses{
				{
					Status:   status.FailedStatus,
					Resource: newPodWithPhase("crashloop-pod", namespace, corev1.PodRunning),
				},
			},
			expectedIdentifier: object.ObjMetadata{
				GroupKind: rsGVK.GroupKind(),
				Name:      name,
				Namespace: namespace,
			},
			expectedStatus: status.FailedStatus,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			fakeReader := fakecr.NewNoopClusterReader()
			fakeMapper := fakemapper.NewFakeRESTMapper()
			podControllerStatusReader := &podControllerStatusReader{
				mapper: fakeMapper,
				statusFunc: func(u *unstructured.Unstructured) (*status.Result, error) {
					return tc.computeStatusResult, tc.computeStatusErr
				},
				statusForGenResourcesFunc: fakeStatusForGenResourcesFunc(tc.genResourceStatuses, nil),
			}

			rs := &unstructured.Unstructured{}
			rs.SetGroupVersionKind(rsGVK)
			rs.SetName(name)
			rs.SetNamespace(namespace)

			resourceStatus, err := podControllerStatusReader.readStatus(context.Background(), fakeReader, rs)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedIdentifier, resourceStatus.Identifier)
			assert.Equal(t, tc.expectedStatus, resourceStatus.Status)
		})
	}
}

func newPodWithPhase(name, namespace string, phase corev1.PodPhase) *unstructured.Unstructured {
	pod := &unstructured.Unstructured{}
	pod.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Pod"})
	pod.SetName(name)
	pod.SetNamespace(namespace)
	_ = unstructured.SetNestedField(pod.Object, string(phase), "status", "phase")
	return pod
}

func newUnschedulablePod(name, namespace string) *unstructured.Unstructured {
	pod := newPodWithPhase(name, namespace, corev1.PodPending)
	_ = unstructured.SetNestedSlice(pod.Object, []interface{}{
		map[string]interface{}{
			"type":   string(corev1.PodScheduled),
			"status": string(corev1.ConditionFalse),
			"reason": corev1.PodReasonUnschedulable,
		},
	}, "status", "conditions")
	return pod
}

func newDeletingPod(name, namespace string) *unstructured.Unstructured {
	pod := newPodWithPhase(name, namespace, corev1.PodRunning)
	pod.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	return pod
}

func fakeStatusForGenResourcesFunc(resourceStatuses event.ResourceStatuses, err error) statusForGenResourcesFunc {
	return func(_ context.Context, _ meta.RESTMapper, _ engine.ClusterReader, _ resourceTypeStatusReader,
		_ *unstructured.Unstructured, _ schema.GroupKind, _ ...string) (event.ResourceStatuses, error) {
		return resourceStatuses, err
	}
}
