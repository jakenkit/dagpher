package dagpher

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Endpoint 保持不变
type Endpoint func(ctx context.Context, req any) (any, error)

// Middleware 接口更新，使用新的 DependencyNode 接口
type Middleware func(node DependencyNode, next Endpoint) Endpoint

// ChainMw 保持不变
func ChainMw(middlewares ...Middleware) Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](node, next)
		}
		return next
	}
}

// TimeoutMW 保持不变
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

// LoggerMW 保持不变
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

// RetryMW 新增的重试中间件
func RetryMW(maxRetries int) Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			var lastErr error

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					fmt.Printf("retry node: %s, attempt: %d/%d\n", node.Name(), attempt, maxRetries)
				}

				resp, err := next(ctx, req)
				if err == nil {
					return resp, nil
				}

				lastErr = err

				// 检查是否应该重试（可以根据错误类型判断）
				if !shouldRetry(err) {
					break
				}

				// 避免最后一次尝试後的不必要等待
				if attempt < maxRetries {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
						// 指数退避
					}
				}
			}

			return nil, fmt.Errorf("node %s failed after %d retries, last error: %w",
				node.Name(), maxRetries, lastErr)
		}
	}
}

// shouldRetry 判断是否应该重试
func shouldRetry(err error) bool {
	// 这里可以根据具体的错误类型来判断
	// 例如：网络错误可以重试，业务逻辑错误不重试
	return true // 简单实现，所有错误都重试
}

// MetricsMW 指标中间件
func MetricsMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			duration := time.Since(start)

			// 这里可以发送指标到监控系统
			status := "success"
			if err != nil {
				status = "error"
			}

			fmt.Printf("METRICS: node=%s, duration=%v, status=%s\n",
				node.Name(), duration, status)

			return resp, err
		}
	}
}

// RecoveryMW 恢复中间件，防止panic导致整个DAG失败
func RecoveryMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("RECOVERY: node %s panicked: %v\nStack: %s\n",
						node.Name(), r, string(debug.Stack()))
				}
			}()

			return next(ctx, req)
		}
	}
}
