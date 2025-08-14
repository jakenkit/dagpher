// Package dagpher provides middleware management and execution utilities for DAG execution.
// This file contains middleware chain processing and execution helpers.
package dagpher

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

type Endpoint func(ctx context.Context, req any) (any, error)

type Middleware func(node DependencyNode, next Endpoint) Endpoint

func ChainMw(middlewares ...Middleware) Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](node, next)
		}
		return next
	}
}

// ExecuteWithMiddleware executes a node with middleware chain applied.
// This is a utility function to avoid code duplication in group and chain execution.
func ExecuteWithMiddleware[C any](middlewares []Middleware, node Node[C], ctx context.Context, c C) error {
	_, err := ChainMw(middlewares...)(node, func(ctx context.Context, in any) (out any, err error) {
		realIn, ok := in.(C)
		if !ok {
			return nil, fmt.Errorf("expected input type %T, got %T", c, in)
		}

		err = node.Exec(ctx, realIn)
		if err != nil {
			return nil, err
		}
		return realIn, nil
	})(ctx, c)
	return err
}

func TimeoutMW(timeout time.Duration) Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			var (
				out    any
				outErr error
				done   = make(chan error, 1)
			)
			go func() {
				defer func() {
					if err := recover(); err != nil {
						done <- fmt.Errorf("timeout mw panic: %v, stacks: %v", err, string(debug.Stack()))
					}
					close(done)
				}()
				out, outErr = next(ctx, req)
			}()

			select {
			case panicErr := <-done:
				if panicErr != nil {
					return nil, panicErr
				}
				return out, outErr
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

func LoggerMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			fmt.Printf("start run node: %s[deps: %v, group_path: %s], start: %s, req: %v\n", node.Name(), node.Dependencies(), GetCurrentGroupPath(ctx), start, req)
			resp, err := next(ctx, req)
			fmt.Printf("end run node: %s[deps: %v, group_path: %s], cost: %s, resp: %v, err: %v\n", node.Name(), node.Dependencies(), GetCurrentGroupPath(ctx), time.Since(start), resp, err)
			return resp, err
		}
	}
}
