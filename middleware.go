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
			fmt.Printf("start run node: %s, start: %s, req: %v\n", node.Name(), start, req)
			resp, err := next(ctx, req)
			fmt.Printf("end run node: %s, cost: %s, resp: %v, err: %v\n", node.Name(), time.Since(start), resp, err)
			return resp, err
		}
	}
}
