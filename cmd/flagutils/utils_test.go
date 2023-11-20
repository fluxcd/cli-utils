// Copyright 2021 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package flagutils

import (
	"fmt"
	"testing"

	"github.com/fluxcd/cli-utils/pkg/inventory"
)

func TestConvertInventoryPolicy(t *testing.T) {
	testcases := []struct {
		value  string
		policy inventory.Policy
		err    error
	}{
		{
			value:  "strict",
			policy: inventory.PolicyMustMatch,
		},
		{
			value:  "adopt",
			policy: inventory.PolicyAdoptIfNoInventory,
		},
		{
			value:  "force-adopt",
			policy: inventory.PolicyAdoptAll,
		},
		{
			value: "random",
			err:   fmt.Errorf("inventory policy must be one of strict, adopt"),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.value, func(t *testing.T) {
			policy, err := ConvertInventoryPolicy(tc.value)
			if tc.err == nil {
				if err != nil {
					t.Errorf("unexpected error %v", err)
				}
				if policy != tc.policy {
					t.Errorf("expected %v but got %v", policy, tc.policy)
				}
			}
			if err == nil && tc.err != nil {
				t.Errorf("expected an error, but not happened")
			}
		})
	}
}
