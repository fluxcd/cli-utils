// Copyright 2022 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package watcher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	"github.com/fluxcd/cli-utils/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/scheme"
)

var deployment1Yaml = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.19.6
        ports:
        - containerPort: 80
`

var deployment1InProgress1Yaml = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.19.6
        ports:
        - containerPort: 80
status:
  observedGeneration: 1
  updatedReplicas: 0
  readyReplicas: 0
  availableReplicas: 0
  replicas: 0
  conditions:
  - reason: NewReplicaSetAvailable
    status: "True"
    type: Progressing
    message: ReplicaSet "nginx-1" is progressing.
  - reason: MinimumReplicasUnavailable
    type: Available
    status: "False"
    message: Deployment does not have minimum availability.
`

var deployment1InProgress2Yaml = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.19.6
        ports:
        - containerPort: 80
status:
  observedGeneration: 1
  updatedReplicas: 1
  readyReplicas: 0
  availableReplicas: 0
  replicas: 1
  conditions:
  - reason: NewReplicaSetAvailable
    status: "True"
    type: Progressing
    message: ReplicaSet "nginx-1" is progressing.
  - reason: MinimumReplicasUnavailable
    type: Available
    status: "False"
    message: Deployment does not have minimum availability.
`

var deployment1CurrentYaml = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.19.6
        ports:
        - containerPort: 80
status:
  observedGeneration: 1
  updatedReplicas: 1
  readyReplicas: 1
  availableReplicas: 1
  replicas: 1
  conditions:
  - reason: NewReplicaSetAvailable
    status: "True"
    type: Progressing
    message: ReplicaSet "nginx-1" has successfully progressed.
  - reason: MinimumReplicasAvailable
    type: Available
    status: "True"
    message: Deployment has minimum availability.
`

var replicaset1Yaml = `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx-1
  generation: 1
  labels:
    app: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
`

var replicaset1InProgress1Yaml = `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx-1
  generation: 1
  labels:
    app: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
status:
  observedGeneration: 1
  replicas: 0
  readyReplicas: 0
  availableReplicas: 0
  fullyLabeledReplicas: 0
`

var replicaset1InProgress2Yaml = `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx-1
  generation: 1
  labels:
    app: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
status:
  observedGeneration: 1
  replicas: 1
  readyReplicas: 0
  availableReplicas: 0
  fullyLabeledReplicas: 1
`

var replicaset1CurrentYaml = `
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx-1
  generation: 1
  labels:
    app: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
status:
  observedGeneration: 1
  replicas: 1
  readyReplicas: 1
  availableReplicas: 1
  fullyLabeledReplicas: 1
`

var pod1Yaml = `
apiVersion: v1
kind: Pod
metadata:
  generation: 1
  name: test
  labels:
    app: nginx
`

var pod1CurrentYaml = `
apiVersion: v1
kind: Pod
metadata:
  generation: 1
  name: test
  labels:
    app: nginx
status:
  conditions:
  - type: Ready 
    status: "True"
  phase: Running
`

func yamlToUnstructured(t *testing.T, yml string) *unstructured.Unstructured {
	m := make(map[string]interface{})
	err := yaml.Unmarshal([]byte(yml), &m)
	if err != nil {
		t.Fatalf("error parsing yaml: %v", err)
		return nil
	}
	return &unstructured.Unstructured{Object: m}
}

func TestDefaultStatusWatcher(t *testing.T) {
	deployment1 := yamlToUnstructured(t, deployment1Yaml)
	deployment1ID := object.UnstructuredToObjMetadata(deployment1)
	deployment1InProgress1 := yamlToUnstructured(t, deployment1InProgress1Yaml)
	deployment1InProgress2 := yamlToUnstructured(t, deployment1InProgress2Yaml)
	deployment1Current := yamlToUnstructured(t, deployment1CurrentYaml)

	replicaset1 := yamlToUnstructured(t, replicaset1Yaml)
	replicaset1ID := object.UnstructuredToObjMetadata(replicaset1)
	replicaset1InProgress1 := yamlToUnstructured(t, replicaset1InProgress1Yaml)
	replicaset1InProgress2 := yamlToUnstructured(t, replicaset1InProgress2Yaml)
	replicaset1Current := yamlToUnstructured(t, replicaset1CurrentYaml)

	pod1 := yamlToUnstructured(t, pod1Yaml)
	pod1ID := object.UnstructuredToObjMetadata(pod1)
	pod1Current := yamlToUnstructured(t, pod1CurrentYaml)

	fakeMapper := testutil.NewFakeRESTMapper(
		appsv1.SchemeGroupVersion.WithKind("Deployment"),
		appsv1.SchemeGroupVersion.WithKind("ReplicaSet"),
		v1.SchemeGroupVersion.WithKind("Pod"),
	)
	deployment1GVR := getGVR(t, fakeMapper, deployment1)
	replicaset1GVR := getGVR(t, fakeMapper, replicaset1)
	pod1GVR := getGVR(t, fakeMapper, pod1)

	// namespace2 := "ns-2"
	// namespace3 := "ns-3"

	pod2 := pod1.DeepCopy()
	pod2.SetNamespace("ns-2")
	pod2.SetName("pod-2")
	pod2ID := object.UnstructuredToObjMetadata(pod2)
	pod2Current := yamlToUnstructured(t, pod1CurrentYaml)
	pod2Current.SetNamespace("ns-2")
	pod2Current.SetName("pod-2")
	pod2GVR := getGVR(t, fakeMapper, pod2)

	pod3 := pod1.DeepCopy()
	pod3.SetNamespace("ns-3")
	pod3.SetName("pod-3")
	pod3ID := object.UnstructuredToObjMetadata(pod3)
	pod3Current := yamlToUnstructured(t, pod1CurrentYaml)
	pod3Current.SetNamespace("ns-3")
	pod3Current.SetName("pod-3")
	pod3GVR := getGVR(t, fakeMapper, pod3)

	testCases := []struct {
		name           string
		ids            object.ObjMetadataSet
		opts           Options
		clusterUpdates []func(*dynamicfake.FakeDynamicClient)
		expectedEvents []event.Event
	}{
		{
			name: "single-namespace pod creation",
			ids: object.ObjMetadataSet{
				pod1ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod1GVR, pod1, pod1.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod1GVR, pod1Current, pod1Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod1ID,
						Status:             status.InProgressStatus,
						Resource:           pod1,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod1ID,
						Status:             status.CurrentStatus,
						Resource:           pod1Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
			},
		},
		{
			name: "single-namespace replicaset creation",
			ids: object.ObjMetadataSet{
				replicaset1ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(replicaset1GVR, replicaset1, replicaset1.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1InProgress1, replicaset1InProgress1.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod1GVR, pod1, pod1.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1InProgress2, replicaset1InProgress2.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod1GVR, pod1Current, pod1Current.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1Current, replicaset1Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         replicaset1ID,
						Status:             status.InProgressStatus,
						Resource:           replicaset1,
						Message:            "Labelled: 0/1",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         replicaset1ID,
						Status:             status.InProgressStatus,
						Resource:           replicaset1InProgress1,
						Message:            "Labelled: 0/1",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: replicaset1ID,
						Status:     status.InProgressStatus,
						Resource:   replicaset1InProgress2,
						Message:    "Available: 0/1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier:         pod1ID,
								Status:             status.InProgressStatus,
								Resource:           pod1,
								Message:            "Pod phase not available",
								GeneratedResources: nil,
							},
						},
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: replicaset1ID,
						Status:     status.CurrentStatus,
						Resource:   replicaset1Current,
						Message:    "ReplicaSet is available. Replicas: 1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier:         pod1ID,
								Status:             status.CurrentStatus,
								Resource:           pod1Current,
								Message:            "Pod is Ready",
								GeneratedResources: nil,
							},
						},
					},
				},
			},
		},
		{
			name: "single-namespace deployment creation",
			ids: object.ObjMetadataSet{
				deployment1ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(deployment1GVR, deployment1, deployment1.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(replicaset1GVR, replicaset1, replicaset1.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1InProgress1, replicaset1InProgress1.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(deployment1GVR, deployment1InProgress1, deployment1InProgress1.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod1GVR, pod1, pod1.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1InProgress2, replicaset1InProgress2.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(deployment1GVR, deployment1InProgress2, deployment1InProgress2.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod1GVR, pod1Current, pod1Current.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(replicaset1GVR, replicaset1Current, replicaset1Current.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Update(deployment1GVR, deployment1Current, deployment1Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         deployment1ID,
						Status:             status.InProgressStatus,
						Resource:           deployment1,
						Message:            "Replicas: 0/1",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: deployment1ID,
						Status:     status.InProgressStatus,
						Resource:   deployment1InProgress1,
						Message:    "Replicas: 0/1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier:         replicaset1ID,
								Status:             status.InProgressStatus,
								Resource:           replicaset1InProgress1,
								Message:            "Labelled: 0/1",
								GeneratedResources: nil,
							},
						},
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: deployment1ID,
						Status:     status.InProgressStatus,
						Resource:   deployment1InProgress2,
						Message:    "Available: 0/1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier: replicaset1ID,
								Status:     status.InProgressStatus,
								Resource:   replicaset1InProgress2,
								Message:    "Available: 0/1",
								GeneratedResources: event.ResourceStatuses{
									{
										Identifier:         pod1ID,
										Status:             status.InProgressStatus,
										Resource:           pod1,
										Message:            "Pod phase not available",
										GeneratedResources: nil,
									},
								},
							},
						},
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: deployment1ID,
						Status:     status.CurrentStatus,
						Resource:   deployment1Current,
						Message:    "Deployment is available. Replicas: 1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier: replicaset1ID,
								Status:     status.CurrentStatus,
								Resource:   replicaset1Current,
								Message:    "ReplicaSet is available. Replicas: 1",
								GeneratedResources: event.ResourceStatuses{
									{
										Identifier:         pod1ID,
										Status:             status.CurrentStatus,
										Resource:           pod1Current,
										Message:            "Pod is Ready",
										GeneratedResources: nil,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "single-namespace deployment deletion",
			ids: object.ObjMetadataSet{
				deployment1ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod1GVR, pod1Current, pod1Current.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Create(replicaset1GVR, replicaset1Current, replicaset1Current.GetNamespace()))
					require.NoError(t, fakeClient.Tracker().Create(deployment1GVR, deployment1Current, deployment1Current.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Delete(pod1GVR, pod1Current.GetNamespace(), pod1Current.GetName()))
					require.NoError(t, fakeClient.Tracker().Delete(replicaset1GVR, replicaset1Current.GetNamespace(), replicaset1Current.GetName()))
					require.NoError(t, fakeClient.Tracker().Delete(deployment1GVR, deployment1Current.GetNamespace(), deployment1Current.GetName()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier: deployment1ID,
						Status:     status.CurrentStatus,
						Resource:   deployment1Current,
						Message:    "Deployment is available. Replicas: 1",
						GeneratedResources: event.ResourceStatuses{
							{
								Identifier: replicaset1ID,
								Status:     status.CurrentStatus,
								Resource:   replicaset1Current,
								Message:    "ReplicaSet is available. Replicas: 1",
								GeneratedResources: event.ResourceStatuses{
									{
										Identifier:         pod1ID,
										Status:             status.CurrentStatus,
										Resource:           pod1Current,
										Message:            "Pod is Ready",
										GeneratedResources: nil,
									},
								},
							},
						},
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         deployment1ID,
						Status:             status.NotFoundStatus,
						Resource:           nil,
						Message:            "Resource not found",
						GeneratedResources: nil,
					},
				},
			},
		},
		{
			name: "multi-namespace pod creation with automatic scope",
			opts: Options{
				RESTScopeStrategy: RESTScopeAutomatic,
			},
			ids: object.ObjMetadataSet{
				pod2ID,
				pod3ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod2GVR, pod2, pod2.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod3GVR, pod3, pod3.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod2GVR, pod2Current, pod2Current.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod3GVR, pod3Current, pod3Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.InProgressStatus,
						Resource:           pod2,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.InProgressStatus,
						Resource:           pod3,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.CurrentStatus,
						Resource:           pod2Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.CurrentStatus,
						Resource:           pod3Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
			},
		},
		{
			name: "multi-namespace pod creation with root scope",
			opts: Options{
				RESTScopeStrategy: RESTScopeRoot,
			},
			ids: object.ObjMetadataSet{
				pod2ID,
				pod3ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod2GVR, pod2, pod2.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod3GVR, pod3, pod3.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod2GVR, pod2Current, pod2Current.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod3GVR, pod3Current, pod3Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.InProgressStatus,
						Resource:           pod2,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.InProgressStatus,
						Resource:           pod3,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.CurrentStatus,
						Resource:           pod2Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.CurrentStatus,
						Resource:           pod3Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
			},
		},
		{
			name: "multi-namespace pod creation with namespace scope",
			opts: Options{
				RESTScopeStrategy: RESTScopeNamespace,
			},
			ids: object.ObjMetadataSet{
				pod2ID,
				pod3ID,
			},
			clusterUpdates: []func(fakeClient *dynamicfake.FakeDynamicClient){
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					// Empty cluster before synchronization.
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod2GVR, pod2, pod2.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Create(pod3GVR, pod3, pod3.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod2GVR, pod2Current, pod2Current.GetNamespace()))
				},
				func(fakeClient *dynamicfake.FakeDynamicClient) {
					require.NoError(t, fakeClient.Tracker().Update(pod3GVR, pod3Current, pod3Current.GetNamespace()))
				},
			},
			expectedEvents: []event.Event{
				{
					Type: event.SyncEvent,
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.InProgressStatus,
						Resource:           pod2,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.InProgressStatus,
						Resource:           pod3,
						Message:            "Pod phase not available",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod2ID,
						Status:             status.CurrentStatus,
						Resource:           pod2Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
				{
					Resource: &event.ResourceStatus{
						Identifier:         pod3ID,
						Status:             status.CurrentStatus,
						Resource:           pod3Current,
						Message:            "Pod is Ready",
						GeneratedResources: nil,
					},
				},
			},
		},
	}

	testTimeout := 10 * time.Second

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			fakeClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme)

			// log fakeClient calls
			fakeClient.PrependReactor("*", "*", func(a clienttesting.Action) (bool, runtime.Object, error) {
				klog.V(3).Infof("FakeDynamicClient: %T{ Verb: %q, Resource: %q, Namespace: %q }",
					a, a.GetVerb(), a.GetResource().Resource, a.GetNamespace())
				return false, nil, nil
			})
			fakeClient.PrependWatchReactor("*", func(a clienttesting.Action) (bool, watch.Interface, error) {
				klog.V(3).Infof("FakeDynamicClient: %T{ Verb: %q, Resource: %q, Namespace: %q }",
					a, a.GetVerb(), a.GetResource().Resource, a.GetNamespace())
				return false, nil, nil
			})

			statusWatcher := NewDefaultStatusWatcher(fakeClient, fakeMapper)
			eventCh := statusWatcher.Watch(ctx, tc.ids, tc.opts)

			nextCh := make(chan struct{})
			defer close(nextCh)

			// Synchronize event consumption and production for predictable test results.
			go func() {
				for _, update := range tc.clusterUpdates {
					<-nextCh
					update(fakeClient)
				}
				// Wait for final event to be handled
				<-nextCh
				// Stop the watcher
				cancel()
			}()

			// Trigger first server update
			nextCh <- struct{}{}

			receivedEvents := []event.Event{}
			for e := range eventCh {
				receivedEvents = append(receivedEvents, e)
				// Trigger next server update
				nextCh <- struct{}{}
			}
			testutil.AssertEqual(t, tc.expectedEvents, receivedEvents)
		})
	}
}

func getGVR(t *testing.T, mapper meta.RESTMapper, obj *unstructured.Unstructured) schema.GroupVersionResource {
	gvk := obj.GroupVersionKind()
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	require.NoError(t, err)
	return mapping.Resource
}

// slowStatusReader simulates a StatusReader that takes a fixed amount of time
// to compute status, mimicking the synchronous API calls made by the real
// deployment/replicaset status readers when fetching generated resources.
type slowStatusReader struct {
	delay       time.Duration
	inFlight    int64
	maxInFlight int64
}

func (s *slowStatusReader) Supports(schema.GroupKind) bool {
	return true
}

func (s *slowStatusReader) ReadStatus(ctx context.Context, _ engine.ClusterReader, id object.ObjMetadata) (*event.ResourceStatus, error) {
	s.trackStart()
	defer s.trackDone()

	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &event.ResourceStatus{
		Identifier: id,
		Status:     status.CurrentStatus,
		Message:    "Current",
	}, nil
}

func (s *slowStatusReader) ReadStatusForObject(ctx context.Context, _ engine.ClusterReader, obj *unstructured.Unstructured) (*event.ResourceStatus, error) {
	s.trackStart()
	defer s.trackDone()

	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	id := object.UnstructuredToObjMetadata(obj)
	return &event.ResourceStatus{
		Identifier: id,
		Status:     status.CurrentStatus,
		Resource:   obj,
		Message:    "Current",
	}, nil
}

func (s *slowStatusReader) trackStart() {
	current := atomic.AddInt64(&s.inFlight, 1)
	for {
		max := atomic.LoadInt64(&s.maxInFlight)
		if current <= max || atomic.CompareAndSwapInt64(&s.maxInFlight, max, current) {
			return
		}
	}
}

func (s *slowStatusReader) trackDone() {
	atomic.AddInt64(&s.inFlight, -1)
}

// TestEventHandlerConcurrency verifies that the informer event handler does not
// block the entire notification pipeline when status computation is slow.
//
// When multiple objects share the same informer (same GroupKind + namespace),
// events should be processed concurrently rather than serially. With serial
// processing, N objects each taking D time would result in ~N*D total latency.
// With concurrent processing, total latency should be close to D.
//
// This test will FAIL before the fix (serial handler) and PASS after (async handler).
func TestEventHandlerConcurrency(t *testing.T) {
	const numPods = 10
	const statusDelay = 100 * time.Millisecond

	fakeMapper := testutil.NewFakeRESTMapper(
		v1.SchemeGroupVersion.WithKind("Pod"),
	)

	pods := make([]*unstructured.Unstructured, numPods)
	ids := make(object.ObjMetadataSet, numPods)
	for i := 0; i < numPods; i++ {
		pod := &unstructured.Unstructured{}
		pod.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Pod"))
		pod.SetNamespace("default")
		pod.SetName(fmt.Sprintf("pod-%d", i))
		pods[i] = pod
		ids[i] = object.UnstructuredToObjMetadata(pod)
	}

	podGVR := getGVR(t, fakeMapper, pods[0])

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme)

	statusWatcher := NewDefaultStatusWatcher(fakeClient, fakeMapper)
	reader := &slowStatusReader{delay: statusDelay}
	statusWatcher.StatusReader = reader

	eventCh := statusWatcher.Watch(ctx, ids, Options{})

	// Wait for the SyncEvent indicating the informer has started watching.
	syncReceived := false
	for e := range eventCh {
		if e.Type == event.SyncEvent {
			syncReceived = true
			break
		}
	}
	require.True(t, syncReceived, "expected SyncEvent before any resource events")

	// Create all pods at once to queue up events in the informer.
	for _, pod := range pods {
		require.NoError(t, fakeClient.Tracker().Create(podGVR, pod.DeepCopy(), pod.GetNamespace()))
	}

	// Collect all ResourceUpdateEvents and measure total elapsed time.
	start := time.Now()
	received := 0
	for received < numPods {
		select {
		case e, ok := <-eventCh:
			if !ok {
				t.Fatalf("event channel closed after receiving %d/%d events", received, numPods)
			}
			if e.Type == event.ResourceUpdateEvent {
				received++
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for events: received %d/%d", received, numPods)
		}
	}
	elapsed := time.Since(start)
	cancel()

	serialTime := time.Duration(numPods) * statusDelay
	t.Logf("Received %d events in %v (serial estimate: %v)", numPods, elapsed, serialTime)
	assert.Greater(t, atomic.LoadInt64(&reader.maxInFlight), int64(1),
		"expected overlapping status computations, got max in-flight=%d",
		atomic.LoadInt64(&reader.maxInFlight))

	assert.Less(t, elapsed, serialTime*3/4,
		"event handler appears too close to serial processing: took %v, expected under %v",
		elapsed, serialTime*3/4)
}
