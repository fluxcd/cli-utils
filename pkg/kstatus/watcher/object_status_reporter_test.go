// Copyright 2026 The Flux Authors.
// SPDX-License-Identifier: Apache-2.0

package watcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	"github.com/fluxcd/cli-utils/pkg/testutil"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/kubectl/pkg/scheme"
)

// blockingRecheckReader reports every watched Pod as unschedulable, which makes
// the reporter schedule a delayed status check. That check calls ReadStatus,
// which blocks until released. It deliberately ignores context cancellation, to
// model a read that completes just as the watch is being stopped.
type blockingRecheckReader struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
}

func (r *blockingRecheckReader) Supports(schema.GroupKind) bool {
	return true
}

func (r *blockingRecheckReader) ReadStatus(_ context.Context, _ engine.ClusterReader, id object.ObjMetadata) (*event.ResourceStatus, error) {
	r.startedOnce.Do(func() { close(r.started) })
	<-r.release
	return &event.ResourceStatus{
		Identifier: id,
		Status:     status.CurrentStatus,
	}, nil
}

func (r *blockingRecheckReader) ReadStatusForObject(_ context.Context, _ engine.ClusterReader, obj *unstructured.Unstructured) (*event.ResourceStatus, error) {
	pod := obj.DeepCopy()
	err := unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{
			"type":   "PodScheduled",
			"status": "False",
			"reason": "Unschedulable",
		},
	}, "status", "conditions")
	if err != nil {
		return nil, err
	}
	return &event.ResourceStatus{
		Identifier: object.UnstructuredToObjMetadata(pod),
		Status:     status.InProgressStatus,
		Resource:   pod,
	}, nil
}

// TestStopWaitsForScheduledStatusCheck verifies that the event channel is not
// closed while a scheduled status check is in flight. Closing it earlier makes
// the check panic with "send on closed channel".
//
// Each case takes at least status.ScheduleWindow, so they run in parallel.
func TestStopWaitsForScheduledStatusCheck(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()

			fakeMapper := testutil.NewFakeRESTMapper(
				v1.SchemeGroupVersion.WithKind("Pod"),
			)
			pod := &unstructured.Unstructured{}
			pod.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Pod"))
			pod.SetNamespace("default")
			pod.SetName("unschedulable")

			fakeClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme)
			require.NoError(t, fakeClient.Tracker().Create(
				getGVR(t, fakeMapper, pod), pod.DeepCopy(), pod.GetNamespace()))

			reader := &blockingRecheckReader{
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			statusWatcher := NewDefaultStatusWatcher(fakeClient, fakeMapper)
			statusWatcher.StatusReader = reader
			statusWatcher.StatusComputeWorkers = workers

			ctx, cancel := context.WithTimeout(context.Background(), status.ScheduleWindow+30*time.Second)
			defer cancel()

			eventCh := statusWatcher.Watch(ctx,
				object.ObjMetadataSet{object.UnstructuredToObjMetadata(pod)}, Options{})

			// Consume events until the scheduled status check starts.
		waitForCheck:
			for {
				select {
				case _, ok := <-eventCh:
					require.True(t, ok, "event channel closed before the status check started")
				case <-reader.started:
					break waitForCheck
				}
			}

			// Stop watching while the check is in flight.
			cancel()

			grace := time.After(500 * time.Millisecond)
		waitForGrace:
			for {
				select {
				case _, ok := <-eventCh:
					require.True(t, ok, "event channel closed while a status check was in flight")
				case <-grace:
					break waitForGrace
				}
			}

			close(reader.release)

			timeout := time.After(10 * time.Second)
			for {
				select {
				case e, ok := <-eventCh:
					if !ok {
						return
					}
					// Only the scheduled check reports Current. Its result
					// must be dropped, because the watch was stopped.
					if e.Type == event.ResourceUpdateEvent && e.Resource != nil {
						require.NotEqual(t, status.CurrentStatus, e.Resource.Status,
							"status check result sent after the watch was stopped")
					}
				case <-timeout:
					t.Fatal("event channel was not closed after the status check finished")
				}
			}
		})
	}
}
