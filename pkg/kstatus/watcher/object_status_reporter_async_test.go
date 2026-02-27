// Copyright 2022 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package watcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
)

type gateStatusReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGateStatusReader() *gateStatusReader {
	return &gateStatusReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *gateStatusReader) Supports(schema.GroupKind) bool {
	return true
}

func (s *gateStatusReader) ReadStatus(context.Context, engine.ClusterReader, object.ObjMetadata) (*event.ResourceStatus, error) {
	return nil, errors.New("ReadStatus not implemented for this test")
}

func (s *gateStatusReader) ReadStatusForObject(_ context.Context, _ engine.ClusterReader, obj *unstructured.Unstructured) (*event.ResourceStatus, error) {
	s.once.Do(func() {
		close(s.started)
	})
	<-s.release
	return &event.ResourceStatus{
		Identifier: object.UnstructuredToObjMetadata(obj),
		Status:     status.CurrentStatus,
		Resource:   obj,
		Message:    "Current",
	}, nil
}

type barrierStatusReader struct {
	started sync.WaitGroup
	release chan struct{}
}

func newBarrierStatusReader(expectedCalls int) *barrierStatusReader {
	r := &barrierStatusReader{
		release: make(chan struct{}),
	}
	r.started.Add(expectedCalls)
	return r
}

func (s *barrierStatusReader) Supports(schema.GroupKind) bool {
	return true
}

func (s *barrierStatusReader) ReadStatus(context.Context, engine.ClusterReader, object.ObjMetadata) (*event.ResourceStatus, error) {
	return nil, errors.New("ReadStatus not implemented for this test")
}

func (s *barrierStatusReader) ReadStatusForObject(_ context.Context, _ engine.ClusterReader, obj *unstructured.Unstructured) (*event.ResourceStatus, error) {
	s.started.Done()
	<-s.release
	return &event.ResourceStatus{
		Identifier: object.UnstructuredToObjMetadata(obj),
		Status:     status.CurrentStatus,
		Resource:   obj,
		Message:    "Current",
	}, nil
}

func (s *barrierStatusReader) WaitUntilAllStarted(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.started.Wait()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for status reads to start")
	}
}

func TestAsyncComputeStatusDoesNotBlockWhenContextCancelled(t *testing.T) {
	reader := newGateStatusReader()
	reporter := &ObjectStatusReporter{
		StatusReader: reader,
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Pod"))
	obj.SetNamespace("default")
	obj.SetName("pod-0")
	obj.SetResourceVersion("1")

	id := object.UnstructuredToObjMetadata(obj)
	eventSeq := uint64(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventCh := make(chan event.Event) // intentionally unread
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var latestEventSeqs sync.Map
	latestEventSeqs.Store(id, eventSeq)

	reporter.asyncComputeStatus(
		ctx,
		eventCh,
		&wg,
		sem,
		&latestEventSeqs,
		&taskManager{},
		id,
		obj,
		eventSeq,
		"Test",
		func(error) {},
	)

	<-reader.started
	cancel()
	close(reader.release)

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("asyncComputeStatus blocked waiting to send on event channel")
	}
}

func TestAsyncComputeStatusDropsStaleSequence(t *testing.T) {
	reader := newBarrierStatusReader(2)
	reporter := &ObjectStatusReporter{
		StatusReader: reader,
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Pod"))
	obj.SetNamespace("default")
	obj.SetName("pod-0")
	obj.SetResourceVersion("1")

	id := object.UnstructuredToObjMetadata(obj)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventCh := make(chan event.Event, 2)
	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup
	var latestEventSeqs sync.Map

	latestEventSeqs.Store(id, uint64(1))
	reporter.asyncComputeStatus(
		ctx,
		eventCh,
		&wg,
		sem,
		&latestEventSeqs,
		&taskManager{},
		id,
		obj,
		uint64(1),
		"Test",
		func(error) {},
	)

	latestEventSeqs.Store(id, uint64(2))
	reporter.asyncComputeStatus(
		ctx,
		eventCh,
		&wg,
		sem,
		&latestEventSeqs,
		&taskManager{},
		id,
		obj,
		uint64(2),
		"Test",
		func(error) {},
	)

	reader.WaitUntilAllStarted(t, 2*time.Second)
	close(reader.release)
	wg.Wait()

	updateEvents := 0
	for {
		select {
		case e := <-eventCh:
			if e.Type == event.ResourceUpdateEvent {
				updateEvents++
			}
		default:
			if updateEvents != 1 {
				t.Fatalf("expected exactly one latest update event, got %d", updateEvents)
			}
			return
		}
	}
}

func TestHandleFatalErrorReturnsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reporter := &ObjectStatusReporter{}
	eventCh := make(chan event.Event) // intentionally unread

	done := make(chan struct{})
	go func() {
		defer close(done)
		reporter.handleFatalError(ctx, eventCh, errors.New("boom"))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleFatalError blocked waiting to send on event channel")
	}
}
