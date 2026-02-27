// Copyright 2022 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package watcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/engine"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
)

// GroupKindNamespace identifies an informer target.
// When used as an informer target, the namespace is optional.
// When the namespace is empty for namespaced resources, all namespaces are watched.
type GroupKindNamespace struct {
	Group     string
	Kind      string
	Namespace string
}

// String returns a serialized form suitable for logging.
func (gkn GroupKindNamespace) String() string {
	return fmt.Sprintf("%s/%s/namespaces/%s",
		gkn.Group, gkn.Kind, gkn.Namespace)
}

func (gkn GroupKindNamespace) GroupKind() schema.GroupKind {
	return schema.GroupKind{Group: gkn.Group, Kind: gkn.Kind}
}

// ObjectStatusReporter reports on updates to objects (instances) using a
// network of informers to watch one or more resources (types).
//
// Unlike SharedIndexInformer, ObjectStatusReporter...
//   - Reports object status.
//   - Can watch multiple resource types simultaneously.
//   - Specific objects can be ignored for efficiency by specifying an ObjectFilter.
//   - Resolves GroupKinds into Resources at runtime, to pick up newly added
//     resources.
//   - Starts and Stops individual watches automaically to reduce errors when a
//     CRD or Namespace is deleted.
//   - Resources can be watched in root-scope mode or namespace-scope mode,
//     allowing the caller to optimize for efficiency or least-privilege.
//   - Gives unschedulable Pods (and objects that generate them) a 15s grace
//     period before reporting them as Failed.
//   - Resets the RESTMapper cache automatically when CRDs are modified.
//
// ObjectStatusReporter is NOT repeatable. It will panic if started more than
// once. If you need a repeatable factory, use DefaultStatusWatcher.
//
// TODO: support detection of added/removed api extensions at runtime
// TODO: Watch CRDs & Namespaces, even if not in the set of IDs.
// TODO: Retry with backoff if in namespace-scoped mode, to allow CRDs & namespaces to be created asynchronously
type ObjectStatusReporter struct {
	// InformerFactory is used to build informers
	InformerFactory *DynamicInformerFactory

	// Mapper is used to map from GroupKind to GroupVersionKind.
	Mapper meta.RESTMapper

	// StatusReader specifies a custom implementation of the
	// engine.StatusReader interface that will be used to compute reconcile
	// status for resource objects.
	StatusReader engine.StatusReader

	// ClusterReader is used to look up generated objects on-demand.
	// Generated objects (ex: Deployment > ReplicaSet > Pod) are sometimes
	// required for computing parent object status, to compensate for
	// controllers that aren't following status conventions.
	ClusterReader engine.ClusterReader

	// StatusComputeWorkers is the maximum number of concurrent goroutines
	// used to compute object status per informer. Status computation may
	// involve synchronous API calls (e.g., fetching generated resources),
	// so this bounds the number of concurrent API calls. Defaults to 8.
	StatusComputeWorkers int

	// GroupKinds is the list of GroupKinds to watch.
	Targets []GroupKindNamespace

	// ObjectFilter is used to decide which objects to ingore.
	ObjectFilter ObjectFilter

	// RESTScope specifies whether to ListAndWatch resources at the namespace
	// or cluster (root) level. Using root scope is more efficient, but
	// namespace scope may require fewer permissions.
	RESTScope meta.RESTScope

	// lock guards modification of the subsequent stateful fields
	lock sync.Mutex

	// gk2gkn maps GKs to GKNs to make it easy/cheap to look up.
	gk2gkn map[schema.GroupKind]map[GroupKindNamespace]struct{}

	// ns2gkn maps Namespaces to GKNs to make it easy/cheap to look up.
	ns2gkn map[string]map[GroupKindNamespace]struct{}

	// informerRefs tracks which informers have been started and stopped
	informerRefs map[GroupKindNamespace]*informerReference

	// context will be cancelled when the reporter should stop.
	context context.Context

	// cancel function that stops the context.
	// This should only be called after the terminal error event has been sent.
	cancel context.CancelFunc

	// funnel multiplexes multiple input channels into one output channel,
	// allowing input channels to be added and removed at runtime.
	funnel *eventFunnel

	started bool
	stopped bool
}

func (w *ObjectStatusReporter) Start(ctx context.Context) <-chan event.Event {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.started {
		panic("ObjectStatusInformer cannot be restarted")
	}

	// Map GroupKinds to sets of GroupKindNamespaces for fast lookups.
	// This is the only time we modify the map.
	// So it should be safe to read from multiple threads after this.
	w.gk2gkn = make(map[schema.GroupKind]map[GroupKindNamespace]struct{})
	for _, gkn := range w.Targets {
		gk := gkn.GroupKind()
		m, found := w.gk2gkn[gk]
		if !found {
			m = make(map[GroupKindNamespace]struct{})
			w.gk2gkn[gk] = m
		}
		m[gkn] = struct{}{}
	}

	// Map namespaces to sets of GroupKindNamespaces for fast lookups.
	// This is the only time we modify the map.
	// So it should be safe to read from multiple threads after this.
	w.ns2gkn = make(map[string]map[GroupKindNamespace]struct{})
	for _, gkn := range w.Targets {
		ns := gkn.Namespace
		m, found := w.ns2gkn[ns]
		if !found {
			m = make(map[GroupKindNamespace]struct{})
			w.ns2gkn[ns] = m
		}
		m[gkn] = struct{}{}
	}

	// Initialize the informer map with references to track their start/stop.
	// This is the only time we modify the map.
	// So it should be safe to read from multiple threads after this.
	w.informerRefs = make(map[GroupKindNamespace]*informerReference, len(w.Targets))
	for _, gkn := range w.Targets {
		w.informerRefs[gkn] = &informerReference{}
	}

	ctx, cancel := context.WithCancel(ctx)
	w.context = ctx
	w.cancel = cancel

	// Use an event funnel to multiplex events through multiple input channels
	// into out output channel. We can't use the normal fan-in pattern, because
	// we need to be able to add and remove new input channels at runtime, as
	// new informers are created and destroyed.
	w.funnel = newEventFunnel(ctx)

	// Send start requests.
	for _, gkn := range w.Targets {
		w.startInformer(gkn)
	}

	w.started = true

	// Block until the event funnel is closed.
	// The event funnel will close after all the informer channels are closed.
	// The informer channels will close after the informers have stopped.
	// The informers will stop after their context is cancelled.
	go func() {
		<-w.funnel.Done()

		w.lock.Lock()
		defer w.lock.Unlock()
		w.stopped = true
	}()

	// Wait until all informers are synced or stopped, then send a SyncEvent.
	syncEventCh := make(chan event.Event)
	err := w.funnel.AddInputChannel(syncEventCh)
	if err != nil {
		// Reporter already stopped.
		return handleFatalError(fmt.Errorf("reporter failed to start: %v", err))
	}
	go func() {
		defer close(syncEventCh)
		// TODO: should we use something less aggressive, like wait.BackoffUntil?
		if cache.WaitForCacheSync(ctx.Done(), w.HasSynced) {
			syncEventCh <- event.Event{
				Type: event.SyncEvent,
			}
		}
	}()

	return w.funnel.OutputChannel()
}

// Stop triggers the cancellation of the reporter context, and closure of the
// event channel without sending an error event.
func (w *ObjectStatusReporter) Stop() {
	klog.V(4).Info("Stopping reporter")
	w.cancel()
}

// HasSynced returns true if all the started informers have been synced.
//
// Use the following to block waiting for synchronization:
// synced := cache.WaitForCacheSync(stopCh, informer.HasSynced)
func (w *ObjectStatusReporter) HasSynced() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.stopped || !w.started {
		return false
	}

	pending := make([]GroupKindNamespace, 0, len(w.informerRefs))
	for gke, informer := range w.informerRefs {
		if informer.HasStarted() && !informer.HasSynced() {
			pending = append(pending, gke)
		}
	}
	if len(pending) > 0 {
		klog.V(5).Infof("Informers pending synchronization: %v", pending)
		return false
	}
	return true
}

// startInformer adds the specified GroupKindNamespace to the start channel to
// be started asynchronously.
func (w *ObjectStatusReporter) startInformer(gkn GroupKindNamespace) {
	ctx, ok := w.informerRefs[gkn].Start(w.context)
	if !ok {
		klog.V(5).Infof("Watch start skipped (already started): %v", gkn)
		// already started
		return
	}
	go w.startInformerWithRetry(ctx, gkn)
}

// stopInformer stops the informer watching the specified GroupKindNamespace.
func (w *ObjectStatusReporter) stopInformer(gkn GroupKindNamespace) {
	w.informerRefs[gkn].Stop()
}

func (w *ObjectStatusReporter) startInformerWithRetry(ctx context.Context, gkn GroupKindNamespace) {
	realClock := &clock.RealClock{}
	// TODO nolint can be removed once https://github.com/kubernetes/kubernetes/issues/118638 is resolved
	backoffManager := wait.NewExponentialBackoffManager(800*time.Millisecond, 30*time.Second, 2*time.Minute, 2.0, 1.0, realClock) //nolint:staticcheck
	retryCtx, retryCancel := context.WithCancel(ctx)

	wait.BackoffUntil(func() {
		err := w.startInformerNow(
			ctx,
			gkn,
		)
		if err != nil {
			if meta.IsNoMatchError(err) {
				// CRD (or api extension) not installed
				// TODO: retry if CRDs are not being watched
				klog.V(3).Infof("Watch start error (blocking until CRD is added): %v: %v", gkn, err)
				// Cancel the parent context, which will stop the retries too.
				w.stopInformer(gkn)
				return
			}

			// Create a temporary input channel to send the error event.
			eventCh := make(chan event.Event)
			defer close(eventCh)
			err := w.funnel.AddInputChannel(eventCh)
			if err != nil {
				// Reporter already stopped.
				// This is fine. 🔥
				klog.V(5).Infof("Informer failed to start: %v", err)
				return
			}
			// Send error event and stop the reporter!
			w.handleFatalError(ctx, eventCh, err)
			return
		}
		// Success! - Stop retrying
		retryCancel()
	}, backoffManager, true, retryCtx.Done())
}

// startInformerNow starts an informer to watch for changes to a
// GroupKindNamespace. Changes are filtered and passed by event channel into the
// funnel. Each update event includes the computed status of the object.
// An error is returned if the informer could not be created.
func (w *ObjectStatusReporter) startInformerNow(
	ctx context.Context,
	gkn GroupKindNamespace,
) error {
	// Look up the mapping for this GroupKind.
	// If it doesn't exist, either delay watching or emit an error.
	gk := gkn.GroupKind()
	mapping, err := w.Mapper.RESTMapping(gk)
	if err != nil {
		// Might be a NoResourceMatchError/NoKindMatchError
		return err
	}

	informer := w.InformerFactory.NewInformer(ctx, mapping, gkn.Namespace)

	w.informerRefs[gkn].SetInformer(informer)

	eventCh := make(chan event.Event)

	// Add this event channel to the output multiplexer
	err = w.funnel.AddInputChannel(eventCh)
	if err != nil {
		// Reporter already stopped.
		return fmt.Errorf("informer failed to build event handler: %w", err)
	}

	var fatalErrorOnce sync.Once
	onFatal := func(err error) {
		fatalErrorOnce.Do(func() {
			w.handleFatalError(ctx, eventCh, err)
		})
	}

	// Handler called when ListAndWatch errors.
	// Custom handler stops the informer if the resource is NotFound (CRD deleted).
	err = informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		w.watchErrorHandler(gkn, onFatal, err)
	})
	if err != nil {
		// Should never happen.
		// Informer can't have started yet. We just created it.
		return fmt.Errorf("failed to set error handler on new informer for %v: %v", mapping.Resource, err)
	}

	workers := w.StatusComputeWorkers
	if workers <= 0 {
		workers = 8
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	var latestEventSeqs sync.Map
	var nextEventSeq atomic.Uint64
	tasks := &taskManager{}

	_, err = informer.AddEventHandler(w.eventHandler(ctx, eventCh, &wg, sem, &latestEventSeqs, &nextEventSeq, tasks, onFatal))
	if err != nil {
		// Should never happen.
		return fmt.Errorf("failed add event handler on new informer for %v: %v", mapping.Resource, err)
	}

	// Start the informer in the background.
	// Informer will be stopped when the context is cancelled.
	go func() {
		klog.V(3).Infof("Watch starting: %v", gkn)
		informer.Run(ctx.Done())
		klog.V(3).Infof("Watch stopped: %v", gkn)
		// Cancel any scheduled re-checks tied to this informer.
		tasks.CancelAll()
		// Wait for all in-flight status computations to finish before
		// closing the event channel, to avoid send-on-closed-channel panics.
		wg.Wait()
		// Cancel again to catch tasks that were scheduled late by goroutines
		// that completed after the first cancellation pass.
		tasks.CancelAll()
		// Wait for all scheduled task goroutines to exit before closing.
		tasks.Wait()
		// Signal to the caller there will be no more events for this GroupKind.
		close(eventCh)
	}()

	return nil
}

func (w *ObjectStatusReporter) forEachTargetWithGroupKind(gk schema.GroupKind, fn func(GroupKindNamespace)) {
	for gkn := range w.gk2gkn[gk] {
		fn(gkn)
	}
}

func (w *ObjectStatusReporter) forEachTargetWithNamespace(ns string, fn func(GroupKindNamespace)) {
	for gkn := range w.ns2gkn[ns] {
		fn(gkn)
	}
}

// readStatusFromObject is a convenience function to read object status with a
// StatusReader using a ClusterReader to retrieve generated objects.
func (w *ObjectStatusReporter) readStatusFromObject(
	ctx context.Context,
	obj *unstructured.Unstructured,
) (*event.ResourceStatus, error) {
	return w.StatusReader.ReadStatusForObject(ctx, w.ClusterReader, obj)
}

// readStatusFromCluster is a convenience function to read object status with a
// StatusReader using a ClusterReader to retrieve the object and its generated
// objects.
func (w *ObjectStatusReporter) readStatusFromCluster(
	ctx context.Context,
	id object.ObjMetadata,
) (*event.ResourceStatus, error) {
	return w.StatusReader.ReadStatus(ctx, w.ClusterReader, id)
}

// deletedStatus builds a ResourceStatus for a deleted object.
//
// StatusReader.ReadStatusForObject doesn't handle nil objects as input. So
// this builds the status manually.
// TODO: find a way to delegate this back to the status package.
func deletedStatus(id object.ObjMetadata) *event.ResourceStatus {
	// Status is always NotFound after deltion.
	// Passed obj represents the last known state, not the current state.
	result := &event.ResourceStatus{
		Identifier: id,
		Status:     status.NotFoundStatus,
		Message:    "Resource not found",
	}

	return &event.ResourceStatus{
		Identifier: id,
		Resource:   nil, // deleted object has no
		Status:     result.Status,
		Message:    result.Message,
		// If deleted with foreground deletion, a finalizer will have blocked
		// deletion until all the generated resources are deleted.
		// TODO: Handle lookup of generated resources when not using foreground deletion.
		GeneratedResources: nil,
	}
}

// eventHandler builds an event handler to compute object status.
//
// Status computation (which may involve synchronous API calls) is dispatched
// to goroutines bounded by the semaphore, so the informer notification
// pipeline is never blocked. A per-informer monotonic sequence counter assigns
// an event sequence number to each object event; goroutines that finish after a
// newer sequence has been observed for the same object will detect staleness
// and drop their result.
//
// NOTE: per-object "latest event wins" behavior is preserved, but events for
// different objects may be observed out-of-order compared to informer arrival.
func (w *ObjectStatusReporter) eventHandler(
	ctx context.Context,
	eventCh chan<- event.Event,
	wg *sync.WaitGroup,
	sem chan struct{},
	latestEventSeqs *sync.Map,
	nextEventSeq *atomic.Uint64,
	taskManager *taskManager,
	onFatal func(error),
) cache.ResourceEventHandler {
	var handler cache.ResourceEventHandlerFuncs

	handler.AddFunc = func(iobj interface{}) {
		if ctx.Err() != nil {
			return
		}

		obj, ok := iobj.(*unstructured.Unstructured)
		if !ok {
			panic(fmt.Sprintf("AddFunc received unexpected object type %T", iobj))
		}
		id := object.UnstructuredToObjMetadata(obj)
		if w.ObjectFilter.Filter(obj) {
			klog.V(7).Infof("Watch Event Skipped: AddFunc: %s", id)
			return
		}
		klog.V(5).Infof("AddFunc: Computing status for object: %s", id)

		// Cancel any scheduled status update for this object.
		taskManager.Cancel(id)

		if object.IsNamespace(obj) {
			klog.V(5).Infof("AddFunc: Namespace added: %v", id)
			w.onNamespaceAdd(obj)
		} else if object.IsCRD(obj) {
			klog.V(5).Infof("AddFunc: CRD added: %v", id)
			w.onCRDAdd(obj)
		}

		eventSeq := nextEventSeq.Add(1)
		latestEventSeqs.Store(id, eventSeq)
		w.asyncComputeStatus(ctx, eventCh, wg, sem, latestEventSeqs, taskManager, id, obj, eventSeq, "AddFunc", onFatal)
	}

	handler.UpdateFunc = func(_, iobj interface{}) {
		if ctx.Err() != nil {
			return
		}

		obj, ok := iobj.(*unstructured.Unstructured)
		if !ok {
			panic(fmt.Sprintf("UpdateFunc received unexpected object type %T", iobj))
		}
		id := object.UnstructuredToObjMetadata(obj)
		if w.ObjectFilter.Filter(obj) {
			klog.V(7).Infof("UpdateFunc: Watch Event Skipped: %s", id)
			return
		}
		klog.V(5).Infof("UpdateFunc: Computing status for object: %s", id)

		// Cancel any scheduled status update for this object.
		taskManager.Cancel(id)

		if object.IsNamespace(obj) {
			klog.V(5).Infof("UpdateFunc: Namespace updated: %v", id)
			w.onNamespaceUpdate(obj)
		} else if object.IsCRD(obj) {
			klog.V(5).Infof("UpdateFunc: CRD updated: %v", id)
			w.onCRDUpdate(obj)
		}

		eventSeq := nextEventSeq.Add(1)
		latestEventSeqs.Store(id, eventSeq)
		w.asyncComputeStatus(ctx, eventCh, wg, sem, latestEventSeqs, taskManager, id, obj, eventSeq, "UpdateFunc", onFatal)
	}

	handler.DeleteFunc = func(iobj interface{}) {
		if ctx.Err() != nil {
			return
		}

		if tombstone, ok := iobj.(cache.DeletedFinalStateUnknown); ok {
			// Last state unknown. Possibly stale.
			// TODO: Should we propagate this uncertainty to the caller?
			iobj = tombstone.Obj
		}
		obj, ok := iobj.(*unstructured.Unstructured)
		if !ok {
			panic(fmt.Sprintf("DeleteFunc received unexpected object type %T", iobj))
		}
		id := object.UnstructuredToObjMetadata(obj)
		if w.ObjectFilter.Filter(obj) {
			klog.V(7).Infof("DeleteFunc: Watch Event Skipped: %s", id)
			return
		}
		klog.V(5).Infof("DeleteFunc: Computing status for object: %s", id)

		// Cancel any scheduled status update for this object.
		taskManager.Cancel(id)

		// Invalidate any in-flight async status computations for this object.
		nextEventSeq.Add(1)
		latestEventSeqs.Delete(id)

		if object.IsNamespace(obj) {
			klog.V(5).Infof("DeleteFunc: Namespace deleted: %v", id)
			w.onNamespaceDelete(obj)
		} else if object.IsCRD(obj) {
			klog.V(5).Infof("DeleteFunc: CRD deleted: %v", id)
			w.onCRDDelete(obj)
		}

		rs := deletedStatus(id)
		klog.V(7).Infof("DeleteFunc: sending update event: %v", rs)
		select {
		case <-ctx.Done():
			return
		case eventCh <- event.Event{
			Type:     event.ResourceUpdateEvent,
			Resource: rs,
		}:
		}
	}

	return handler
}

func (w *ObjectStatusReporter) asyncComputeStatus(
	ctx context.Context,
	eventCh chan<- event.Event,
	wg *sync.WaitGroup,
	sem chan struct{},
	latestEventSeqs *sync.Map,
	taskManager *taskManager,
	id object.ObjMetadata,
	obj *unstructured.Unstructured,
	eventSeq uint64,
	handlerName string,
	onFatal func(error),
) {
	objCopy := obj.DeepCopy()

	// We launch one goroutine per informer event and bound active work with sem.
	// This favors simple informer integration over a full worker-pool lifecycle.
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return
		}

		// Bail early if the context is cancelled, to avoid unnecessary work.
		if ctx.Err() != nil {
			return
		}

		rs, err := w.readStatusFromObject(ctx, objCopy)
		if err != nil {
			onFatal(fmt.Errorf("failed to compute object status: %s: %w", id, err))
			return
		}

		// Drop stale events before scheduling delayed re-checks.
		if !isLatestEventSequence(latestEventSeqs, id, eventSeq) {
			if cur, ok := latestEventSeqs.Load(id); ok {
				klog.V(7).Infof("%s: skipping stale event for %v (seq=%d, latest=%v)", handlerName, id, eventSeq, cur)
			} else {
				klog.V(7).Infof("%s: skipping stale event for %v (seq=%d, latest=<deleted>)", handlerName, id, eventSeq)
			}
			return
		}

		scheduled := false
		if isObjectUnschedulable(rs) {
			klog.V(5).Infof("%s: object unschedulable: %v", handlerName, id)
			taskManager.Schedule(ctx, id, status.ScheduleWindow,
				w.newStatusCheckTaskFunc(ctx, eventCh, id, onFatal))
			scheduled = true
		}

		// Re-check staleness after scheduling to avoid orphaned delayed tasks.
		if !isLatestEventSequence(latestEventSeqs, id, eventSeq) {
			if scheduled {
				taskManager.Cancel(id)
			}
			if cur, ok := latestEventSeqs.Load(id); ok {
				klog.V(7).Infof("%s: skipping stale event for %v after scheduling (seq=%d, latest=%v)", handlerName, id, eventSeq, cur)
			} else {
				klog.V(7).Infof("%s: skipping stale event for %v after scheduling (seq=%d, latest=<deleted>)", handlerName, id, eventSeq)
			}
			return
		}

		// There is still a narrow TOCTOU window between this stale check and
		// the send where a delete can race; watcher events are eventually
		// consistent and consumers should treat a later NotFound as authoritative.
		klog.V(7).Infof("%s: sending update event: %v", handlerName, rs)
		select {
		case <-ctx.Done():
			return
		case eventCh <- event.Event{
			Type:     event.ResourceUpdateEvent,
			Resource: rs,
		}:
		}
	}()
}

func isLatestEventSequence(latestEventSeqs *sync.Map, id object.ObjMetadata, eventSeq uint64) bool {
	cur, ok := latestEventSeqs.Load(id)
	if !ok {
		return false
	}
	currentSeq, ok := cur.(uint64)
	if !ok {
		return false
	}
	return currentSeq == eventSeq
}

// onCRDAdd handles creating a new informer to watch the new resource type.
func (w *ObjectStatusReporter) onCRDAdd(obj *unstructured.Unstructured) {
	gk, found := object.GetCRDGroupKind(obj)
	if !found {
		id := object.UnstructuredToObjMetadata(obj)
		klog.Warningf("Invalid CRD added: missing group and/or kind: %v", id)
		// Don't return an error, because this should not inturrupt the task queue.
		// TODO: Allow non-fatal errors to be reported using a specific error type.
		return
	}
	klog.V(3).Infof("CRD added for %s", gk)

	klog.V(3).Info("Resetting RESTMapper")
	// Reset mapper to invalidate cache.
	meta.MaybeResetRESTMapper(w.Mapper)

	w.forEachTargetWithGroupKind(gk, func(gkn GroupKindNamespace) {
		w.startInformer(gkn)
	})
}

// onCRDUpdate handles creating a new informer to watch the updated resource type.
func (w *ObjectStatusReporter) onCRDUpdate(newObj *unstructured.Unstructured) {
	gk, found := object.GetCRDGroupKind(newObj)
	if !found {
		id := object.UnstructuredToObjMetadata(newObj)
		klog.Warningf("Invalid CRD updated: missing group and/or kind: %v", id)
		// Don't return an error, because this should not inturrupt the task queue.
		// TODO: Allow non-fatal errors to be reported using a specific error type.
		return
	}
	klog.V(3).Infof("CRD updated for %s", gk)

	klog.V(3).Info("Resetting RESTMapper")
	// Reset mapper to invalidate cache.
	meta.MaybeResetRESTMapper(w.Mapper)

	w.forEachTargetWithGroupKind(gk, func(gkn GroupKindNamespace) {
		w.startInformer(gkn)
	})
}

// onCRDDelete handles stopping the informer watching the deleted resource type.
func (w *ObjectStatusReporter) onCRDDelete(oldObj *unstructured.Unstructured) {
	gk, found := object.GetCRDGroupKind(oldObj)
	if !found {
		id := object.UnstructuredToObjMetadata(oldObj)
		klog.Warningf("Invalid CRD deleted: missing group and/or kind: %v", id)
		// Don't return an error, because this should not inturrupt the task queue.
		// TODO: Allow non-fatal errors to be reported using a specific error type.
		return
	}
	klog.V(3).Infof("CRD deleted for %s", gk)

	w.forEachTargetWithGroupKind(gk, func(gkn GroupKindNamespace) {
		w.stopInformer(gkn)
	})

	klog.V(3).Info("Resetting RESTMapper")
	// Reset mapper to invalidate cache.
	meta.MaybeResetRESTMapper(w.Mapper)
}

// onNamespaceAdd handles creating new informers to watch this namespace.
func (w *ObjectStatusReporter) onNamespaceAdd(obj *unstructured.Unstructured) {
	if w.RESTScope == meta.RESTScopeRoot {
		// When watching resources across all namespaces,
		// we don't need to start or stop any
		// namespace-specific informers.
		return
	}
	namespace := obj.GetName()
	w.forEachTargetWithNamespace(namespace, func(gkn GroupKindNamespace) {
		w.startInformer(gkn)
	})
}

// onNamespaceUpdate handles creating new informers to watch this namespace.
func (w *ObjectStatusReporter) onNamespaceUpdate(obj *unstructured.Unstructured) {
	if w.RESTScope == meta.RESTScopeRoot {
		// When watching resources across all namespaces,
		// we don't need to start or stop any
		// namespace-specific informers.
		return
	}
	namespace := obj.GetName()
	w.forEachTargetWithNamespace(namespace, func(gkn GroupKindNamespace) {
		w.startInformer(gkn)
	})
}

// onNamespaceDelete handles stopping informers watching this namespace.
func (w *ObjectStatusReporter) onNamespaceDelete(obj *unstructured.Unstructured) {
	if w.RESTScope == meta.RESTScopeRoot {
		// When watching resources across all namespaces,
		// we don't need to start or stop any
		// namespace-specific informers.
		return
	}
	namespace := obj.GetName()
	w.forEachTargetWithNamespace(namespace, func(gkn GroupKindNamespace) {
		w.stopInformer(gkn)
	})
}

// newStatusCheckTaskFunc returns a taskFund that reads the status of an object
// from the cluster and sends it over the event channel.
//
// This method should only be used for generated resource objects, as it's much
// slower at scale than watching the resource for updates.
func (w *ObjectStatusReporter) newStatusCheckTaskFunc(
	ctx context.Context,
	eventCh chan<- event.Event,
	id object.ObjMetadata,
	onFatal func(error),
) taskFunc {
	return func() {
		klog.V(5).Infof("Re-reading object status: %s", id)
		// check again
		rs, err := w.readStatusFromCluster(ctx, id)
		if err != nil {
			// Send error event and stop the reporter!
			// TODO: retry N times before terminating
			onFatal(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case eventCh <- event.Event{
			Type:     event.ResourceUpdateEvent,
			Resource: rs,
		}:
		}
	}
}

func (w *ObjectStatusReporter) handleFatalError(ctx context.Context, eventCh chan<- event.Event, err error) {
	klog.V(5).Infof("Reporter error: %v", err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	select {
	case <-ctx.Done():
		return
	case eventCh <- event.Event{
		Type:  event.ErrorEvent,
		Error: err,
	}:
	}
	w.Stop()
}

// watchErrorHandler logs errors and cancels the informer for this GroupKind
// if the NotFound error is received, which usually means the CRD was deleted.
// Based on DefaultWatchErrorHandler from k8s.io/client-go@v0.23.2/tools/cache/reflector.go
func (w *ObjectStatusReporter) watchErrorHandler(gkn GroupKindNamespace, onFatal func(error), err error) {
	switch {
	// Stop channel closed
	case err == io.EOF:
		klog.V(5).Infof("ListAndWatch error (termination expected): %v: %v", gkn, err)

	// Watch connection closed
	case err == io.ErrUnexpectedEOF:
		klog.V(1).Infof("ListAndWatch error (retry expected): %v: %v", gkn, err)

	// Context done
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		klog.V(5).Infof("ListAndWatch error (termination expected): %v: %v", gkn, err)

	// resourceVersion too old
	case apierrors.IsResourceExpired(err):
		// Keep retrying
		klog.V(5).Infof("ListAndWatch error (retry expected): %v: %v", gkn, err)

	// Resource unregistered (DEPRECATED, see NotFound)
	case apierrors.IsGone(err):
		klog.V(5).Infof("ListAndWatch error (retry expected): %v: %v", gkn, err)

	// Resource not registered
	case apierrors.IsNotFound(err):
		klog.V(3).Infof("ListAndWatch error (termination expected): %v: stopping all informers for this GroupKind: %v", gkn, err)
		w.forEachTargetWithGroupKind(gkn.GroupKind(), func(gkn GroupKindNamespace) {
			w.stopInformer(gkn)
		})

	// Insufficient permissions
	case apierrors.IsForbidden(err):
		klog.V(3).Infof("ListAndWatch error (termination expected): %v: stopping all informers: %v", gkn, err)
		onFatal(err)

	// Unexpected error
	default:
		klog.Warningf("ListAndWatch error (retry expected): %v: %v", gkn, err)
	}
}

// informerReference tracks informer lifecycle.
type informerReference struct {
	// lock guards the subsequent stateful fields
	lock sync.Mutex

	informer cache.SharedIndexInformer
	context  context.Context
	cancel   context.CancelFunc
	started  bool
}

// Start returns a wrapped context that can be cancelled.
// Returns nil & false if already started.
func (ir *informerReference) Start(ctx context.Context) (context.Context, bool) {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	if ir.started {
		return nil, false
	}

	ctx, cancel := context.WithCancel(ctx)
	ir.context = ctx
	ir.cancel = cancel
	ir.started = true

	return ctx, true
}

func (ir *informerReference) SetInformer(informer cache.SharedIndexInformer) {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	ir.informer = informer
}

func (ir *informerReference) HasSynced() bool {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	if !ir.started {
		return false
	}
	if ir.informer == nil {
		return false
	}
	return ir.informer.HasSynced()
}

func (ir *informerReference) HasStarted() bool {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	return ir.started
}

// Stop cancels the context, if it's been started.
func (ir *informerReference) Stop() {
	ir.lock.Lock()
	defer ir.lock.Unlock()

	if !ir.started {
		return
	}

	ir.cancel()
	ir.started = false
	ir.context = nil
}

type taskFunc func()

type scheduledTask struct {
	cancel context.CancelFunc
}

// taskManager manages a set of tasks with object identifiers.
// This makes starting and stopping the tasks thread-safe.
type taskManager struct {
	lock        sync.Mutex
	wg          sync.WaitGroup
	cancelFuncs map[object.ObjMetadata]*scheduledTask
}

func (tm *taskManager) Schedule(parentCtx context.Context, id object.ObjMetadata, delay time.Duration, task taskFunc) {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	if tm.cancelFuncs == nil {
		tm.cancelFuncs = make(map[object.ObjMetadata]*scheduledTask)
	}

	taskRef, found := tm.cancelFuncs[id]
	if found {
		// Cancel the existing scheduled task and replace it.
		taskRef.cancel()
	}

	taskCtx, cancel := context.WithTimeout(context.Background(), delay)
	taskRef = &scheduledTask{cancel: cancel}
	tm.cancelFuncs[id] = taskRef
	tm.wg.Add(1)

	go func(taskRef *scheduledTask) {
		defer tm.wg.Done()
		defer tm.removeIfCurrent(id, taskRef)

		klog.V(5).Infof("Task scheduled (%v) for object (%s)", delay, id)
		select {
		case <-parentCtx.Done():
			// stop waiting
			cancel()
		case <-taskCtx.Done():
			if taskCtx.Err() == context.DeadlineExceeded {
				klog.V(5).Infof("Task executing (after %v) for object (%v)", delay, id)
				task()
			}
			// else stop waiting
		}
	}(taskRef)
}

func (tm *taskManager) Cancel(id object.ObjMetadata) {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	taskRef, found := tm.cancelFuncs[id]
	if !found {
		// already cancelled or not added
		return
	}
	delete(tm.cancelFuncs, id)
	taskRef.cancel()
	if len(tm.cancelFuncs) == 0 {
		tm.cancelFuncs = nil
	}
}

func (tm *taskManager) CancelAll() {
	tm.lock.Lock()
	if len(tm.cancelFuncs) == 0 {
		tm.lock.Unlock()
		return
	}

	taskRefs := make([]*scheduledTask, 0, len(tm.cancelFuncs))
	for id, taskRef := range tm.cancelFuncs {
		taskRefs = append(taskRefs, taskRef)
		delete(tm.cancelFuncs, id)
	}
	tm.cancelFuncs = nil
	tm.lock.Unlock()

	for _, taskRef := range taskRefs {
		taskRef.cancel()
	}
}

func (tm *taskManager) Wait() {
	tm.wg.Wait()
}

func (tm *taskManager) removeIfCurrent(id object.ObjMetadata, taskRef *scheduledTask) {
	tm.lock.Lock()
	defer tm.lock.Unlock()

	current, found := tm.cancelFuncs[id]
	if !found || current != taskRef {
		return
	}

	delete(tm.cancelFuncs, id)
	if len(tm.cancelFuncs) == 0 {
		tm.cancelFuncs = nil
	}
}
