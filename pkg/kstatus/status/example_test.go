// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package status_test

import (
	"strings"
	"testing"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/testutil"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestExampleCompute(t *testing.T) {
	deploymentManifest := `
apiVersion: apps/v1
kind: Deployment
metadata:
   name: test
   generation: 1
   namespace: qual
status:
   observedGeneration: 1
   updatedReplicas: 1
   readyReplicas: 1
   availableReplicas: 1
   replicas: 1
   conditions:
    - type: Progressing
      status: "True"
      reason: NewReplicaSetAvailable
    - type: Available
      status: "True"
`
	deployment := testutil.YamlToUnstructured(t, deploymentManifest)

	res, err := status.Compute(deployment)
	assert.NoError(t, err)

	assert.Equal(t, status.Status("Current"), res.Status)
}

func TestExampleAugment(t *testing.T) {
	deploymentManifest := `
apiVersion: apps/v1
kind: Deployment
metadata:
   name: test
   generation: 1
   namespace: qual
status:
   observedGeneration: 1
   updatedReplicas: 1
   readyReplicas: 1
   availableReplicas: 1
   replicas: 1
   conditions:
    - type: Progressing
      status: "True"
      reason: NewReplicaSetAvailable
    - type: Available
      status: "True"
`
	deployment := testutil.YamlToUnstructured(t, deploymentManifest)

	err := status.Augment(deployment)
	assert.NoError(t, err)

	b, err := yaml.Marshal(deployment.Object)
	assert.NoError(t, err)

	receivedManifest := strings.TrimSpace(string(b))
	expectedManifest := strings.TrimSpace(`
apiVersion: apps/v1
kind: Deployment
metadata:
  generation: 1
  name: test
  namespace: qual
status:
  availableReplicas: 1
  conditions:
  - reason: NewReplicaSetAvailable
    status: "True"
    type: Progressing
  - status: "True"
    type: Available
  observedGeneration: 1
  readyReplicas: 1
  replicas: 1
  updatedReplicas: 1
`)

	assert.Equal(t, expectedManifest, receivedManifest)
}

func TestExampleStringObservedGeneration(t *testing.T) {
	deploymentManifest := `
apiVersion: apps/v1
kind: Deployment
metadata:
   name: test
   generation: 1
   namespace: qual
status:
   observedGeneration: "1"
   updatedReplicas: 1
   readyReplicas: 1
   availableReplicas: 1
   replicas: 1
   conditions:
    - type: Progressing
      status: "True"
      reason: NewReplicaSetAvailable
    - type: Available
      status: "True"
`
	deployment := testutil.YamlToUnstructured(t, deploymentManifest)

	res, err := status.Compute(deployment)
	assert.NoError(t, err)

	assert.Equal(t, status.Status("Current"), res.Status)
}

func TestExampleStringObservedGenerationBad(t *testing.T) {
	deploymentManifest := `
apiVersion: apps/v1
kind: Deployment
metadata:
   name: test
   generation: 1
   namespace: qual
status:
   observedGeneration: "foo"
   updatedReplicas: 1
   readyReplicas: 1
   availableReplicas: 1
   replicas: 1
   conditions:
    - type: Progressing
      status: "True"
      reason: NewReplicaSetAvailable
    - type: Available
      status: "True"
`
	deployment := testutil.YamlToUnstructured(t, deploymentManifest)

	_, err := status.Compute(deployment)
	assert.Error(t, err)
}
