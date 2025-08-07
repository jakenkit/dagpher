package dagpher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type TestContext struct {
	mu             sync.RWMutex
	log            []string
	executionOrder []string
}

func (c *TestContext) Log(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, msg)
}

func (c *TestContext) GetLog() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	logCopy := make([]string, len(c.log))
	copy(logCopy, c.log)
	return logCopy
}

func (c *TestContext) AddExecution(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executionOrder = append(c.executionOrder, name)
}

func (c *TestContext) GetExecutionOrder() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	orderCopy := make([]string, len(c.executionOrder))
	copy(orderCopy, c.executionOrder)
	return orderCopy
}

func TestGroupExecution(t *testing.T) {
	t.Run("simple group with one node", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("simple_group_with_one_node")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		group.AddNode(nodeA)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed"}, testCtx.GetLog())
	})

	t.Run("group with dependencies", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("group_with_dependencies")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A")
		group.AddNode(nodeA)
		group.AddNode(nodeB)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed", "B executed"}, testCtx.GetLog())
	})

	t.Run("nested subgroups", func(t *testing.T) {
		testCtx := &TestContext{}
		rootGroup := NewGroup[*TestContext]("root_group")

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		rootGroup.AddNode(nodeA)

		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		})
		// A -> subgroup1 -> C
		// subgroup1 contains B
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}, "subgroup1") // C depends on the whole subgroup1

		subGroup1 := NewGroup[*TestContext]("subgroup1", "A")
		subGroup1.AddNode(nodeB)

		rootGroup.AddNode(subGroup1.AsNode())
		rootGroup.AddNode(nodeC)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(rootGroup, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed", "B executed", "C executed"}, testCtx.GetLog())
	})

	t.Run("duplicate node in different groups not panics", func(t *testing.T) {
		rootGroup := NewGroup[*TestContext]("root_group")
		subGroup := NewGroup[*TestContext]("subgroup")

		nodeA1 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeA2 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })

		rootGroup.AddNode(nodeA1)
		subGroup.AddNode(nodeA2)
		rootGroup.AddNode(subGroup.AsNode())

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(rootGroup, sem)
		require.NoError(t, executor.Build())
	})

	t.Run("middleware execution order", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("test_group")

		mw1 := func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("global mw1 start")
				res, err := next(ctx, req)
				testCtx.Log("global mw1 end")
				return res, err
			}
		}
		mw2 := func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("node mw2 start")
				res, err := next(ctx, req)
				testCtx.Log("node mw2 end")
				return res, err
			}
		}

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		group.AddNode(nodeA, WithMiddlewares(mw2))

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem, mw1)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		expectedLog := []string{
			"global mw1 start",
			"node mw2 start",
			"A executed",
			"node mw2 end",
			"global mw1 end",
		}
		assert.Equal(t, expectedLog, testCtx.GetLog())
	})

	t.Run("loop variable capture", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("loop_group")

		// Add multiple nodes to test if the loop variable was captured correctly.
		nodes := []Node[*TestContext]{
			NewNode("A", func(ctx context.Context, c *TestContext) error { c.Log("A"); return nil }),
			NewNode("B", func(ctx context.Context, c *TestContext) error { c.Log("B"); return nil }),
			NewNode("C", func(ctx context.Context, c *TestContext) error { c.Log("C"); return nil }),
		}
		for _, n := range nodes {
			group.AddNode(n)
		}

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		// The order is not guaranteed, so we check for the presence of all logs.
		logs := testCtx.GetLog()
		assert.ElementsMatch(t, []string{"A", "B", "C"}, logs)
	})
}

func TestConcurrentSafety(t *testing.T) {
	t.Run("concurrent execution with cancellation", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("cancellation_group")

		// Add nodes with artificial delays
		for i := 0; i < 10; i++ {
			node := NewNode(fmt.Sprintf("slow_node_%d", i), func(ctx context.Context, c *TestContext) error {
				select {
				case <-time.After(100 * time.Millisecond):
					c.Log(fmt.Sprintf("slow_node_%d completed", i))
					return nil
				case <-ctx.Done():
					c.Log(fmt.Sprintf("slow_node_%d cancelled", i))
					return ctx.Err()
				}
			})
			group.AddNode(node)
		}

		require.NoError(t, group.Build())

		// Start execution and cancel after a short time
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := group.Exec(ctx, testCtx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")

		// Verify some nodes were cancelled
		logs := testCtx.GetLog()
		cancelledCount := 0
		for _, log := range logs {
			if strings.Contains(log, "cancelled") {
				cancelledCount++
			}
		}
		assert.Greater(t, cancelledCount, 0)
	})

	t.Run("race condition in dependency resolution", func(t *testing.T) {
		const numRuns = 50

		for run := 0; run < numRuns; run++ {
			testCtx := &TestContext{}
			group := NewGroup[*TestContext]("race_group")

			// Create a complex dependency chain
			nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
				c.AddExecution("A")
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
				c.AddExecution("B")
				return nil
			}, "A")

			nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
				c.AddExecution("C")
				return nil
			}, "A")

			nodeD := NewNode("D", func(ctx context.Context, c *TestContext) error {
				c.AddExecution("D")
				return nil
			}, "B", "C")

			group.AddNode(nodeA)
			group.AddNode(nodeB)
			group.AddNode(nodeC)
			group.AddNode(nodeD)

			sem := semaphore.NewWeighted(2)
			group.SetGlobalSem(sem)

			require.NoError(t, group.Build())
			require.NoError(t, group.Exec(context.Background(), testCtx))

			execOrder := testCtx.GetExecutionOrder()
			require.Len(t, execOrder, 4)

			// Verify dependency constraints
			aIndex := findIndex(execOrder, "A")
			bIndex := findIndex(execOrder, "B")
			cIndex := findIndex(execOrder, "C")
			dIndex := findIndex(execOrder, "D")

			assert.Less(t, aIndex, bIndex, "A should execute before B")
			assert.Less(t, aIndex, cIndex, "A should execute before C")
			assert.Less(t, bIndex, dIndex, "B should execute before D")
			assert.Less(t, cIndex, dIndex, "C should execute before D")
		}
	})

	t.Run("concurrent group nesting", func(t *testing.T) {
		testCtx := &TestContext{}
		rootGroup := NewGroup[*TestContext]("root")

		// Create multiple sub-groups concurrently
		var wg sync.WaitGroup
		const numSubGroups = 5
		wg.Add(numSubGroups)

		for i := 0; i < numSubGroups; i++ {
			go func(id int) {
				defer wg.Done()
				subGroup := NewGroup[*TestContext](fmt.Sprintf("sub_%d", id))

				node := NewNode(fmt.Sprintf("node_%d", id), func(ctx context.Context, c *TestContext) error {
					c.Log(fmt.Sprintf("sub_node_%d executed", id))
					return nil
				})

				subGroup.AddNode(node)
				rootGroup.AddGroup(subGroup)
			}(i)
		}

		wg.Wait()

		require.NoError(t, rootGroup.Build())
		require.NoError(t, rootGroup.Exec(context.Background(), testCtx))

		logs := testCtx.GetLog()
		assert.Len(t, logs, numSubGroups)
	})
}

func findIndex(slice []string, target string) int {
	for i, v := range slice {
		if v == target {
			return i
		}
	}
	return -1
}
