// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package printer

import (
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/collector"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/object"
)

// PrintData records data required for printing
type PrintData struct {
	Identifiers object.ObjMetadataSet
	InvNameMap  map[object.ObjMetadata]string
	StatusSet   map[string]bool
}

// Printer defines an interface for outputting information about status of
// resources. Different implementations allow output formats tailored to
// different use cases.
type Printer interface {

	// Print tells the printer to start outputting data. The stop parameter
	// is a channel that the caller will use to signal to the printer that it
	// needs to stop and shut down. The channel returned can be used by the
	// printer implementation to signal that it has outputted all the data it
	// needs to, and that it has completed shutting down. The latter is important
	// to make sure the printer has a chance to output all data before the
	// program terminates.
	Print(ch <-chan event.Event, identifiers object.ObjMetadataSet, cancelFunc collector.ObserverFunc) error
}
