package executor

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// WrapWithSemaphore wraps an execution function with semaphore control.
// If sem is nil, the function executes without semaphore control.
// This is a utility function to avoid code duplication across the codebase.
func WrapWithSemaphore[C any](sem *semaphore.Weighted, exec func(context.Context, C) error) func(context.Context, C) error {
	if sem == nil {
		return exec
	}

	return func(ctx context.Context, c C) error {
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer sem.Release(1)

		return exec(ctx, c)
	}
}