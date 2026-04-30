// Copyright 2026 The Flux Authors.
// SPDX-License-Identifier: Apache-2.0

// Command controllerruntime is a no-op binary that exists solely to force CI
// to build controller-runtime against this module's dependency versions, so
// upstream incompatibilities (e.g. a Kubernetes bump that breaks
// controller-runtime/pkg/cache) are caught here instead of downstream.
// See https://github.com/fluxcd/cli-utils/issues/32.
package main

import (
	_ "sigs.k8s.io/controller-runtime"
	_ "sigs.k8s.io/controller-runtime/pkg/cache"
	_ "sigs.k8s.io/controller-runtime/pkg/client"
	_ "sigs.k8s.io/controller-runtime/pkg/manager"
)

func main() {}
