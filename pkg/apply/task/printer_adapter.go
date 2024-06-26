// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package task

import (
	"fmt"
	"io"

	"github.com/fluxcd/cli-utils/pkg/apply/event"
	"github.com/fluxcd/cli-utils/pkg/object"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
)

// KubectlPrinterAdapter is a workaround for capturing progress from
// ApplyOptions. ApplyOptions were originally meant to print progress
// directly using a configurable printer. The KubectlPrinterAdapter
// plugs into ApplyOptions as a ToPrinter function, but instead of
// printing the info, it emits it as an event on the provided channel.
type KubectlPrinterAdapter struct {
	ch        chan<- event.Event
	groupName string
}

// resourcePrinterImpl implements the ResourcePrinter interface. But
// instead of printing, it emits information on the provided channel.
type resourcePrinterImpl struct {
	applyStatus event.ApplyEventStatus
	ch          chan<- event.Event
	groupName   string
}

// PrintObj takes the provided object and operation and emits
// it on the channel.
func (r *resourcePrinterImpl) PrintObj(obj runtime.Object, _ io.Writer) error {
	id, err := object.RuntimeToObjMeta(obj)
	if err != nil {
		return err
	}
	r.ch <- event.Event{
		Type: event.ApplyType,
		ApplyEvent: event.ApplyEvent{
			GroupName:  r.groupName,
			Identifier: id,
			Status:     r.applyStatus,
			Resource:   obj.(*unstructured.Unstructured),
		},
	}
	return nil
}

type toPrinterFunc func(string) (printers.ResourcePrinter, error)

// toPrinterFunc returns a function of type toPrinterFunc. This
// is the type required by the ApplyOptions.
func (p *KubectlPrinterAdapter) toPrinterFunc() toPrinterFunc {
	return func(operation string) (printers.ResourcePrinter, error) {
		applyStatus, err := kubectlOperationToApplyStatus(operation)
		return &resourcePrinterImpl{
			ch:          p.ch,
			applyStatus: applyStatus,
			groupName:   p.groupName,
		}, err
	}
}

func kubectlOperationToApplyStatus(operation string) (event.ApplyEventStatus, error) {
	switch operation {
	case "serverside-applied":
		return event.ApplySuccessful, nil
	case "created":
		return event.ApplySuccessful, nil
	case "unchanged":
		return event.ApplySuccessful, nil
	case "configured":
		return event.ApplySuccessful, nil
	default:
		return event.ApplyEventStatus(0), fmt.Errorf("unknown operation %s", operation)
	}
}
