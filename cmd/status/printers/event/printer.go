// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package event

import (
	"fmt"
	"strings"

	"github.com/fluxcd/cli-utils/cmd/status/printers/printer"
	"github.com/fluxcd/cli-utils/pkg/apply/event"
	"github.com/fluxcd/cli-utils/pkg/common"
	"github.com/fluxcd/cli-utils/pkg/kstatus/polling/collector"
	pollevent "github.com/fluxcd/cli-utils/pkg/kstatus/polling/event"
	"github.com/fluxcd/cli-utils/pkg/object"
	"github.com/fluxcd/cli-utils/pkg/print/list"
	"github.com/fluxcd/cli-utils/pkg/printers/events"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

// Printer implements the Printer interface and outputs the resource
// status information as a list of events as they happen.
type Printer struct {
	Formatter list.Formatter
	IOStreams genericclioptions.IOStreams
	Data      *printer.PrintData
}

// NewPrinter returns a new instance of the eventPrinter.
func NewPrinter(ioStreams genericclioptions.IOStreams, printData *printer.PrintData) *Printer {
	return &Printer{
		Formatter: events.NewFormatter(ioStreams, common.DryRunNone),
		IOStreams: ioStreams,
		Data:      printData,
	}
}

// Print takes an event channel and outputs the status events on the channel
// until the channel is closed. The provided cancelFunc is consulted on
// every event and is responsible for stopping the poller when appropriate.
// This function will block.
func (ep *Printer) Print(ch <-chan pollevent.Event, identifiers object.ObjMetadataSet,
	cancelFunc collector.ObserverFunc) error {
	coll := collector.NewResourceStatusCollector(identifiers)
	// The actual work is done by the collector, which will invoke the
	// callback on every event. In the callback we print the status
	// information and call the cancelFunc which is responsible for
	// stopping the poller at the correct time.
	done := coll.ListenWithObserver(ch, collector.ObserverFunc(
		func(statusCollector *collector.ResourceStatusCollector, e pollevent.Event) {
			err := ep.printStatusEvent(e)
			if err != nil {
				panic(err)
			}
			cancelFunc(statusCollector, e)
		}),
	)
	// Listen to the channel until it is closed.
	var err error
	for msg := range done {
		err = msg.Err
	}
	return err
}

func (ep *Printer) printStatusEvent(se pollevent.Event) error {
	switch se.Type {
	case pollevent.ResourceUpdateEvent:
		id := se.Resource.Identifier
		var invName string
		var ok bool
		if invName, ok = ep.Data.InvNameMap[id]; !ok {
			return fmt.Errorf("%s: resource not found", id)
		}
		// filter out status that are not assigned
		statusString := se.Resource.Status.String()
		if _, ok := ep.Data.StatusSet[strings.ToLower(statusString)]; len(ep.Data.StatusSet) != 0 && !ok {
			return nil
		}
		_, err := fmt.Fprintf(ep.IOStreams.Out, "%s/%s/%s/%s is %s: %s\n", invName,
			strings.ToLower(id.GroupKind.String()), id.Namespace, id.Name, statusString, se.Resource.Message)
		return err
	case pollevent.ErrorEvent:
		return ep.Formatter.FormatErrorEvent(event.ErrorEvent{
			Err: se.Error,
		})
	}
	return nil
}
