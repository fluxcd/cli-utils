// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/fluxcd/cli-utils/pkg/apply"
	"github.com/fluxcd/cli-utils/pkg/apply/event"
	"github.com/fluxcd/cli-utils/pkg/common"
	"github.com/fluxcd/cli-utils/pkg/inventory"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	"github.com/fluxcd/cli-utils/pkg/testutil"
	"github.com/fluxcd/cli-utils/test/e2e/e2eutil"
	"github.com/fluxcd/cli-utils/test/e2e/invconfig"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func dryRunTest(ctx context.Context, c client.Client, invConfig invconfig.InventoryConfig, inventoryName, namespaceName string) {
	By("Apply with DryRun")
	applier := invConfig.ApplierFactoryFunc()
	inventoryID := fmt.Sprintf("%s-%s", inventoryName, namespaceName)

	inventoryInfo := invconfig.CreateInventoryInfo(invConfig, inventoryName, namespaceName, inventoryID)

	namespace1Name := fmt.Sprintf("%s-ns1", namespaceName)

	fields := struct{ Namespace string }{Namespace: namespace1Name}
	namespace1Obj := e2eutil.TemplateToUnstructured(namespaceTemplate, fields)
	podBObj := e2eutil.TemplateToUnstructured(podBTemplate, fields)

	// Dependency order: podB -> namespace1
	// Apply order: namespace1, podB
	resources := []*unstructured.Unstructured{
		namespace1Obj,
		podBObj,
	}

	applierEvents := e2eutil.RunCollect(applier.Run(ctx, inventoryInfo, resources, apply.ApplierOptions{
		ReconcileTimeout: 2 * time.Minute,
		EmitStatusEvents: true,
		DryRunStrategy:   common.DryRunClient,
	}))

	expEvents := []testutil.ExpEvent{
		{
			// InitTask
			EventType: event.InitType,
			InitEvent: &testutil.ExpInitEvent{},
		},
		{
			// InvAddTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-add-0",
				Type:      event.Started,
			},
		},
		{
			// InvAddTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-add-0",
				Type:      event.Finished,
			},
		},
		{
			// ApplyTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.ApplyAction,
				GroupName: "apply-0",
				Type:      event.Started,
			},
		},
		{
			// Create namespace
			EventType: event.ApplyType,
			ApplyEvent: &testutil.ExpApplyEvent{
				GroupName:  "apply-0",
				Status:     event.ApplySuccessful,
				Identifier: object.UnstructuredToObjMetadata(namespace1Obj),
				Error:      nil,
			},
		},
		{
			// ApplyTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.ApplyAction,
				GroupName: "apply-0",
				Type:      event.Finished,
			},
		},
		{
			// ApplyTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.ApplyAction,
				GroupName: "apply-1",
				Type:      event.Started,
			},
		},
		{
			// Create pod
			EventType: event.ApplyType,
			ApplyEvent: &testutil.ExpApplyEvent{
				GroupName:  "apply-1",
				Status:     event.ApplySuccessful,
				Identifier: object.UnstructuredToObjMetadata(podBObj),
				Error:      nil,
			},
		},
		{
			// ApplyTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.ApplyAction,
				GroupName: "apply-1",
				Type:      event.Finished,
			},
		},
		// No Wait Tasks for Dry Run
		{
			// InvSetTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-set-0",
				Type:      event.Started,
			},
		},
		{
			// InvSetTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-set-0",
				Type:      event.Finished,
			},
		},
	}
	receivedEvents := testutil.EventsToExpEvents(applierEvents)

	// handle optional async NotFound StatusEvent for pod
	expected := testutil.ExpEvent{
		EventType: event.StatusType,
		StatusEvent: &testutil.ExpStatusEvent{
			Identifier: object.UnstructuredToObjMetadata(podBObj),
			Status:     status.NotFoundStatus,
			Error:      nil,
		},
	}
	receivedEvents, _ = testutil.RemoveEqualEvents(receivedEvents, expected)

	// handle optional async NotFound StatusEvent for namespace
	expected = testutil.ExpEvent{
		EventType: event.StatusType,
		StatusEvent: &testutil.ExpStatusEvent{
			Identifier: object.UnstructuredToObjMetadata(namespace1Obj),
			Status:     status.NotFoundStatus,
			Error:      nil,
		},
	}
	receivedEvents, _ = testutil.RemoveEqualEvents(receivedEvents, expected)

	Expect(receivedEvents).To(testutil.Equal(expEvents))

	By("Verify pod NotFound")
	e2eutil.AssertUnstructuredDoesNotExist(ctx, c, podBObj)

	By("Verify inventory NotFound")
	invConfig.InvNotExistsFunc(ctx, c, inventoryName, namespaceName, inventoryID)

	By("Apply")
	e2eutil.RunWithNoErr(applier.Run(ctx, inventoryInfo, resources, apply.ApplierOptions{
		ReconcileTimeout: 2 * time.Minute,
	}))

	By("Verify pod created")
	e2eutil.AssertUnstructuredExists(ctx, c, podBObj)

	By("Verify inventory size")
	invConfig.InvSizeVerifyFunc(ctx, c, inventoryName, namespaceName, inventoryID, 2, 2)

	By("Destroy with DryRun")
	destroyer := invConfig.DestroyerFactoryFunc()

	destroyerEvents := e2eutil.RunCollect(destroyer.Run(ctx, inventoryInfo, apply.DestroyerOptions{
		InventoryPolicy:  inventory.PolicyAdoptIfNoInventory,
		EmitStatusEvents: true,
		DryRunStrategy:   common.DryRunClient,
	}))

	expEvents = []testutil.ExpEvent{
		{
			// InitTask
			EventType: event.InitType,
			InitEvent: &testutil.ExpInitEvent{},
		},
		{
			// PruneTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.DeleteAction,
				GroupName: "prune-0",
				Type:      event.Started,
			},
		},
		{
			// Delete pod
			EventType: event.DeleteType,
			DeleteEvent: &testutil.ExpDeleteEvent{
				GroupName:  "prune-0",
				Status:     event.DeleteSuccessful,
				Identifier: object.UnstructuredToObjMetadata(podBObj),
				Error:      nil,
			},
		},
		{
			// PruneTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.DeleteAction,
				GroupName: "prune-0",
				Type:      event.Finished,
			},
		},
		{
			// PruneTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.DeleteAction,
				GroupName: "prune-1",
				Type:      event.Started,
			},
		},
		{
			// Delete namespace
			EventType: event.DeleteType,
			DeleteEvent: &testutil.ExpDeleteEvent{
				GroupName:  "prune-1",
				Status:     event.DeleteSuccessful,
				Identifier: object.UnstructuredToObjMetadata(namespace1Obj),
				Error:      nil,
			},
		},
		{
			// PruneTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.DeleteAction,
				GroupName: "prune-1",
				Type:      event.Finished,
			},
		},
		// No Wait Tasks for Dry Run
		{
			// DeleteInvTask start
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-delete-or-update-0",
				Type:      event.Started,
			},
		},
		{
			// DeleteInvTask finished
			EventType: event.ActionGroupType,
			ActionGroupEvent: &testutil.ExpActionGroupEvent{
				Action:    event.InventoryAction,
				GroupName: "inventory-delete-or-update-0",
				Type:      event.Finished,
			},
		},
	}
	Expect(testutil.EventsToExpEvents(destroyerEvents)).To(testutil.Equal(expEvents))

	By("Verify pod still exists")
	e2eutil.AssertUnstructuredExists(ctx, c, podBObj)

	By("Destroy")
	e2eutil.RunWithNoErr(destroyer.Run(ctx, inventoryInfo, apply.DestroyerOptions{
		InventoryPolicy: inventory.PolicyAdoptIfNoInventory,
	}))

	By("Verify pod deleted")
	e2eutil.AssertUnstructuredDoesNotExist(ctx, c, podBObj)

	By("Verify inventory deleted")
	invConfig.InvNotExistsFunc(ctx, c, inventoryName, namespaceName, inventoryID)
}
