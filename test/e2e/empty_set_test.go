// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"

	"github.com/fluxcd/cli-utils/pkg/apply"
	"github.com/fluxcd/cli-utils/pkg/apply/event"
	"github.com/fluxcd/cli-utils/pkg/inventory"
	"github.com/fluxcd/cli-utils/pkg/testutil"
	"github.com/fluxcd/cli-utils/test/e2e/e2eutil"
	"github.com/fluxcd/cli-utils/test/e2e/invconfig"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//nolint:dupl // expEvents similar to other tests
func emptySetTest(ctx context.Context, c client.Client, invConfig invconfig.InventoryConfig, inventoryName, namespaceName string) {
	By("Apply zero resources")
	applier := invConfig.ApplierFactoryFunc()

	inventoryID := fmt.Sprintf("%s-%s", inventoryName, namespaceName)
	inventoryInfo := invConfig.InvWrapperFunc(invConfig.FactoryFunc(inventoryName, namespaceName, inventoryID))

	resources := []*unstructured.Unstructured{}

	applierEvents := e2eutil.RunCollect(applier.Run(ctx, inventoryInfo, resources, apply.ApplierOptions{
		EmitStatusEvents: true,
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
	Expect(testutil.EventsToExpEvents(applierEvents)).To(testutil.Equal(expEvents))

	By("Verify inventory created")
	invConfig.InvSizeVerifyFunc(ctx, c, inventoryName, namespaceName, inventoryID, 0, 0)

	By("Destroy zero resources")
	destroyer := invConfig.DestroyerFactoryFunc()

	options := apply.DestroyerOptions{InventoryPolicy: inventory.PolicyAdoptIfNoInventory}
	destroyerEvents := e2eutil.RunCollect(destroyer.Run(ctx, inventoryInfo, options))

	expEvents = []testutil.ExpEvent{
		{
			// InitTask
			EventType: event.InitType,
			InitEvent: &testutil.ExpInitEvent{},
		},
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

	By("Verify inventory deleted")
	invConfig.InvNotExistsFunc(ctx, c, inventoryName, namespaceName, inventoryID)
}
