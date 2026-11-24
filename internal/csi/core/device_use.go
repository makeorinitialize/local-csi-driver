// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package core

import (
	"context"
	"errors"
)

func BeginPhysical(ctx context.Context, backend any, method, id string) (context.Context, func(bool) error, error) {
	if gate, ok := backend.(interface {
		BeginNative(context.Context, string, string, string) (context.Context, func(bool) error, error)
	}); ok {
		return gate.BeginNative(ctx, method, id, "")
	}
	return ctx, func(bool) error { return nil }, nil
}

func BeginPreparation(ctx context.Context, backend any, method, id, node string, block bool) (context.Context, func(bool) error, error) {
	if gate, ok := backend.(interface {
		BeginPreparation(context.Context, string, string, string, bool) (context.Context, func(bool) error, error)
	}); ok {
		return gate.BeginPreparation(ctx, method, id, node, block)
	}
	return ctx, func(bool) error { return nil }, nil
}

func CompleteNative(finish func(bool) error, result *error) {
	if err := finish(*result == nil); err != nil {
		*result = errors.Join(*result, err)
	}
}
