// Copyright 2021 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package invconfig

import (
	"context"

	"github.com/fluxcd/cli-utils/pkg/apply"
	"github.com/fluxcd/cli-utils/pkg/common"
	"github.com/fluxcd/cli-utils/pkg/inventory"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewConfigMapTypeInvConfig(cfg *rest.Config) InventoryConfig {
	return InventoryConfig{
		ClientConfig:         cfg,
		Strategy:             inventory.LabelStrategy,
		FactoryFunc:          cmInventoryManifest,
		InvWrapperFunc:       inventory.WrapInventoryInfoObj,
		ApplierFactoryFunc:   newDefaultInvApplierFactory(cfg),
		DestroyerFactoryFunc: newDefaultInvDestroyerFactory(cfg),
		InvSizeVerifyFunc:    defaultInvSizeVerifyFunc,
		InvCountVerifyFunc:   defaultInvCountVerifyFunc,
		InvNotExistsFunc:     defaultInvNotExistsFunc,
	}
}

func newDefaultInvApplierFactory(cfg *rest.Config) applierFactoryFunc {
	cfgPtrCopy := cfg
	return func() *apply.Applier {
		return newApplier(inventory.ClusterClientFactory{
			StatusPolicy: inventory.StatusPolicyAll,
		}, cfgPtrCopy)
	}
}

func newDefaultInvDestroyerFactory(cfg *rest.Config) destroyerFactoryFunc {
	cfgPtrCopy := cfg
	return func() *apply.Destroyer {
		return newDestroyer(inventory.ClusterClientFactory{
			StatusPolicy: inventory.StatusPolicyAll,
		}, cfgPtrCopy)
	}
}

func defaultInvNotExistsFunc(ctx context.Context, c client.Client, name, namespace, id string) {
	var cmList v1.ConfigMapList
	err := c.List(ctx, &cmList,
		client.MatchingLabels(map[string]string{common.InventoryLabel: id}),
		client.InNamespace(namespace))
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	gomega.Expect(cmList.Items).To(gomega.HaveLen(0), "expected inventory list to be empty")
}

func defaultInvSizeVerifyFunc(ctx context.Context, c client.Client, name, namespace, id string, specCount, _ int) {
	var cmList v1.ConfigMapList
	err := c.List(ctx, &cmList,
		client.MatchingLabels(map[string]string{common.InventoryLabel: id}),
		client.InNamespace(namespace))
	gomega.Expect(err).WithOffset(1).ToNot(gomega.HaveOccurred(), "listing ConfigMap inventory from cluster")

	gomega.Expect(len(cmList.Items)).WithOffset(1).To(gomega.Equal(1), "number of inventory objects by label")

	data := cmList.Items[0].Data
	gomega.Expect(len(data)).WithOffset(1).To(gomega.Equal(specCount), "inventory spec.data length")

	// Don't validate status size.
	// ConfigMap provider uses inventory.StatusPolicyNone.
}

func defaultInvCountVerifyFunc(ctx context.Context, c client.Client, namespace string, count int) {
	var cmList v1.ConfigMapList
	err := c.List(ctx, &cmList, client.InNamespace(namespace), client.HasLabels{common.InventoryLabel})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(len(cmList.Items)).To(gomega.Equal(count))
}
