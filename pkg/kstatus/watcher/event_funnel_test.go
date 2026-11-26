// Copyright 2026 The Flux Authors.
// SPDX-License-Identifier: Apache-2.0

package watcher

import (
	"context"
	"testing"
	"time"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/stretchr/testify/require"
)

// TestEventFunnelClosesWithoutConsumer verifies that, once the context is
// cancelled, the funnel doesn't block senders or stay open because the
// consumer stopped reading its output channel.
func TestEventFunnelClosesWithoutConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	funnel := newEventFunnel(ctx)

	inCh := make(chan event.Event)
	require.NoError(t, funnel.AddInputChannel(inCh))

	// Stop the funnel without ever reading its output channel.
	cancel()

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		defer close(inCh)
		for range 3 {
			inCh <- event.Event{Type: event.ResourceUpdateEvent}
		}
	}()

	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("sender blocked after the funnel context was cancelled")
	}

	select {
	case <-funnel.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("funnel was not closed after its context was cancelled and its inputs were closed")
	}
}
