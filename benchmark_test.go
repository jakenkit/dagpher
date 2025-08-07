package dagpher

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

type BenchmarkContext struct {
	counter int64
}

func (c *BenchmarkContext) Increment() {
	atomic.AddInt64(&c.counter, 1)
}

func (c *BenchmarkContext) GetCounter() int64 {
	return atomic.LoadInt64(&c.counter)
}

func BenchmarkDagExecution(b *testing.B) {
	b.Run("linear_chain", func(b *testing.B) {
		const chainLength = 100

		benchCtx := &BenchmarkContext{}

		for i := 0; i < b.N; i++ {
			group := NewGroup[*BenchmarkContext]("linear_chain")

			// Create a linear chain of dependencies
			var prevNodeName string
			for j := 0; j < chainLength; j++ {
				nodeName := fmt.Sprintf("node_%d", j)
				var deps []string
				if prevNodeName != "" {
					deps = []string{prevNodeName}
				}

				node := NewNode(nodeName, func(ctx context.Context, c *BenchmarkContext) error {
					c.Increment()
					return nil
				}, deps...)

				group.AddNode(node)
				prevNodeName = nodeName
			}

			b.StartTimer()
			group.Build()
			group.Exec(context.Background(), benchCtx)
			b.StopTimer()

			// Reset counter for next iteration
			atomic.StoreInt64(&benchCtx.counter, 0)
		}
	})

	b.Run("parallel_execution", func(b *testing.B) {
		const numNodes = 100

		benchCtx := &BenchmarkContext{}

		for i := 0; i < b.N; i++ {
			group := NewGroup[*BenchmarkContext]("parallel_group")

			// Create independent parallel nodes
			for j := 0; j < numNodes; j++ {
				nodeName := fmt.Sprintf("parallel_node_%d", j)
				node := NewNode(nodeName, func(ctx context.Context, c *BenchmarkContext) error {
					c.Increment()
					// Simulate some work
					time.Sleep(time.Microsecond)
					return nil
				})
				group.AddNode(node)
			}

			// Use semaphore to control concurrency
			group.SetMaxGoNum(runtime.NumCPU())

			b.StartTimer()
			group.Build()
			group.Exec(context.Background(), benchCtx)
			b.StopTimer()

			atomic.StoreInt64(&benchCtx.counter, 0)
		}
	})

	b.Run("complex_dag", func(b *testing.B) {
		const layers = 5
		const nodesPerLayer = 20

		benchCtx := &BenchmarkContext{}

		for i := 0; i < b.N; i++ {
			group := NewGroup[*BenchmarkContext]("complex_dag")

			// Create a diamond-shaped DAG with multiple layers
			nodeNames := make([][]string, layers)

			for layer := 0; layer < layers; layer++ {
				nodeNames[layer] = make([]string, nodesPerLayer)

				for nodeIdx := 0; nodeIdx < nodesPerLayer; nodeIdx++ {
					nodeName := fmt.Sprintf("layer_%d_node_%d", layer, nodeIdx)
					nodeNames[layer][nodeIdx] = nodeName

					var deps []string
					if layer > 0 {
						// Depend on some nodes from previous layer
						depCount := min(3, nodesPerLayer) // Depend on up to 3 nodes from previous layer
						for d := 0; d < depCount; d++ {
							depIdx := (nodeIdx + d) % nodesPerLayer
							deps = append(deps, nodeNames[layer-1][depIdx])
						}
					}

					node := NewNode(nodeName, func(ctx context.Context, c *BenchmarkContext) error {
						c.Increment()
						return nil
					}, deps...)

					group.AddNode(node)
				}
			}

			group.SetMaxGoNum(runtime.NumCPU() * 2)

			b.StartTimer()
			group.Build()
			group.Exec(context.Background(), benchCtx)
			b.StopTimer()

			atomic.StoreInt64(&benchCtx.counter, 0)
		}
	})

	b.Run("nested_groups", func(b *testing.B) {
		const numGroups = 10
		const nodesPerGroup = 10

		benchCtx := &BenchmarkContext{}

		for i := 0; i < b.N; i++ {
			rootGroup := NewGroup[*BenchmarkContext]("root_group")

			for g := 0; g < numGroups; g++ {
				subGroup := NewGroup[*BenchmarkContext](fmt.Sprintf("sub_group_%d", g))

				for n := 0; n < nodesPerGroup; n++ {
					nodeName := fmt.Sprintf("sub_node_%d_%d", g, n)
					node := NewNode(nodeName, func(ctx context.Context, c *BenchmarkContext) error {
						c.Increment()
						return nil
					})
					subGroup.AddNode(node)
				}

				rootGroup.AddGroup(subGroup)
			}

			rootGroup.SetMaxGoNum(runtime.NumCPU())

			b.StartTimer()
			rootGroup.Build()
			rootGroup.Exec(context.Background(), benchCtx)
			b.StopTimer()

			atomic.StoreInt64(&benchCtx.counter, 0)
		}
	})

	b.Run("semaphore_contention", func(b *testing.B) {
		const numNodes = 1000
		const semLimit = 10

		benchCtx := &BenchmarkContext{}

		for i := 0; i < b.N; i++ {
			group := NewGroup[*BenchmarkContext]("semaphore_group")
			sem := semaphore.NewWeighted(semLimit)
			group.SetGlobalSem(sem)

			for j := 0; j < numNodes; j++ {
				nodeName := fmt.Sprintf("sem_node_%d", j)
				node := NewNode(nodeName, func(ctx context.Context, c *BenchmarkContext) error {
					c.Increment()
					// Simulate work that would benefit from limiting concurrency
					time.Sleep(time.Microsecond * 10)
					return nil
				})
				group.AddNode(node)
			}

			b.StartTimer()
			group.Build()
			group.Exec(context.Background(), benchCtx)
			b.StopTimer()

			atomic.StoreInt64(&benchCtx.counter, 0)
		}
	})
}

func BenchmarkMemoryAllocation(b *testing.B) {
	b.Run("node_creation", func(b *testing.B) {
		//benchCtx := &BenchmarkContext{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			node := NewNode(fmt.Sprintf("node_%d", i), func(ctx context.Context, c *BenchmarkContext) error {
				c.Increment()
				return nil
			})
			_ = node
		}
	})

	b.Run("group_building", func(b *testing.B) {
		const numNodes = 100

		for i := 0; i < b.N; i++ {
			group := NewGroup[*BenchmarkContext](fmt.Sprintf("group_%d", i))

			for j := 0; j < numNodes; j++ {
				node := NewNode(fmt.Sprintf("node_%d", j), func(ctx context.Context, c *BenchmarkContext) error {
					return nil
				})
				group.AddNode(node)
			}

			b.StartTimer()
			group.Build()
			b.StopTimer()
		}
	})
}
