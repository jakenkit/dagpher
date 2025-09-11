package dagpher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type TestContext struct {
	results chan string
	counter int32
}

func NewTestContext(bufferSize int) *TestContext {
	return &TestContext{
		results: make(chan string, bufferSize),
	}
}

func (c *TestContext) Log(msg string) {
	c.results <- msg
}

func (c *TestContext) AddExecution(name string) {
	c.results <- name
}

func (c *TestContext) GetResults() []string {
	close(c.results)
	var results []string
	for res := range c.results {
		results = append(results, res)
	}
	return results
}

func (c *TestContext) Inc() {
	atomic.AddInt32(&c.counter, 1)
}

func (c *TestContext) Count() int {
	return int(atomic.LoadInt32(&c.counter))
}

func TestGroupExecution(t *testing.T) {
	t.Run("simple group with one node", func(t *testing.T) {
		testCtx := NewTestContext(1)
		group := NewGroup[*TestContext]("simple_group_with_one_node")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		group.AddNode(nodeA)

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed"}, testCtx.GetResults())
	})

	t.Run("group with dependencies", func(t *testing.T) {
		testCtx := NewTestContext(2)
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

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))
		results := testCtx.GetResults()
		assert.Contains(t, results, "A executed")
		assert.Contains(t, results, "B executed")
		assert.True(t, findIndex(results, "A executed") < findIndex(results, "B executed"))
	})

	t.Run("nested subgroups", func(t *testing.T) {
		testCtx := NewTestContext(3)
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

		require.NoError(t, rootGroup.Build())
		require.NoError(t, rootGroup.Exec(context.Background(), testCtx))
		results := testCtx.GetResults()
		assert.Contains(t, results, "A executed")
		assert.Contains(t, results, "B executed")
		assert.Contains(t, results, "C executed")

		aIndex := findIndex(results, "A executed")
		bIndex := findIndex(results, "B executed")
		cIndex := findIndex(results, "C executed")

		// A must be before B (as B is in a group that depends on A)
		// A must be before C (as C depends on subgroup1 which depends on A)
		// B must be before C (as C depends on subgroup1 which contains B)
		assert.True(t, aIndex < bIndex)
		assert.True(t, aIndex < cIndex)
		assert.True(t, bIndex < cIndex)
	})

	t.Run("duplicate node in different groups not panics", func(t *testing.T) {
		rootGroup := NewGroup[*TestContext]("root_group")
		subGroup := NewGroup[*TestContext]("subgroup")

		nodeA1 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeA2 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })

		rootGroup.AddNode(nodeA1)
		subGroup.AddNode(nodeA2)
		rootGroup.AddNode(subGroup.AsNode())

		require.NoError(t, rootGroup.Build())
	})

	t.Run("duplicate node name in same group panics", func(t *testing.T) {
		group := NewGroup[*TestContext]("group")
		nodeA1 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeA2 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })

		group.AddNode(nodeA1)
		group.AddNode(nodeA2)

		err := group.Build()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("node with error stops execution of dependent nodes", func(t *testing.T) {
		testCtx := NewTestContext(3)
		group := NewGroup[*TestContext]("error_group")
		expectedErr := errors.New("node A failed")

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return expectedErr
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A") // B depends on A
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}) // C is independent

		group.AddNode(nodeA)
		group.AddNode(nodeB)
		group.AddNode(nodeC)

		require.NoError(t, group.Build())
		err := group.Exec(context.Background(), testCtx)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)

		results := testCtx.GetResults()
		assert.Contains(t, results, "A executed", "A should have executed")
		//assert.Contains(t, results, "C executed", "C should execute as it is independent")
		assert.NotContains(t, results, "B executed", "B should not execute because its dependency failed")
	})

	t.Run("circular dependency detection", func(t *testing.T) {
		t.Run("A -> B -> A", func(t *testing.T) {
			group := NewGroup[*TestContext]("cycle_group")
			nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil }, "B")
			nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error { return nil }, "A")
			group.AddNode(nodeA)
			group.AddNode(nodeB)
			err := group.Build()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cycle detected")
		})

		t.Run("A -> B -> C -> A", func(t *testing.T) {
			group := NewGroup[*TestContext]("cycle_group")
			nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil }, "C")
			nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error { return nil }, "A")
			nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error { return nil }, "B")
			group.AddNode(nodeA)
			group.AddNode(nodeB)
			group.AddNode(nodeC)
			err := group.Build()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cycle detected")
		})
	})

	t.Run("dependency on non-existent node", func(t *testing.T) {
		group := NewGroup[*TestContext]("invalid_dep_group")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil }, "NonExistent")
		group.AddNode(nodeA)
		err := group.Build()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NonExistent")
	})

	t.Run("empty group execution", func(t *testing.T) {
		testCtx := NewTestContext(1)
		group := NewGroup[*TestContext]("empty_group")
		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))
		assert.Empty(t, testCtx.GetResults())
	})

	t.Run("middleware execution order", func(t *testing.T) {
		testCtx := NewTestContext(5)
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
		group.AddMiddleware(mw1)

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))

		expectedLog := []string{
			"global mw1 start",
			"node mw2 start",
			"A executed",
			"node mw2 end",
			"global mw1 end",
		}
		assert.Equal(t, expectedLog, testCtx.GetResults())
	})

	t.Run("loop variable capture", func(t *testing.T) {
		testCtx := NewTestContext(3)
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

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))

		// The order is not guaranteed, so we check for the presence of all logs.
		logs := testCtx.GetResults()
		assert.ElementsMatch(t, []string{"A", "B", "C"}, logs)
	})

	t.Run("concurrency control with SetMaxGoNum", func(t *testing.T) {
		testCtx := NewTestContext(10)
		group := NewGroup[*TestContext]("concurrency_limit_group")
		group.SetMaxGoNum(2)

		var maxConcurrent int32
		var runningCount int32

		for i := 0; i < 5; i++ {
			nodeName := fmt.Sprintf("node-%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				atomic.AddInt32(&runningCount, 1)
				currentRunning := atomic.LoadInt32(&runningCount)
				if currentRunning > atomic.LoadInt32(&maxConcurrent) {
					atomic.StoreInt32(&maxConcurrent, currentRunning)
				}
				time.Sleep(50 * time.Millisecond)
				c.Log(nodeName)
				atomic.AddInt32(&runningCount, -1)
				return nil
			}))
		}

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))

		assert.Equal(t, 5, len(testCtx.GetResults()))
		assert.Equal(t, int32(2), maxConcurrent, "Max concurrency should be limited to 2")
	})

	t.Run("context propagation", func(t *testing.T) {
		type key string
		var myKey key = "my_key"
		expectedValue := "my_value"

		testCtx := NewTestContext(1)
		group := NewGroup[*TestContext]("context_group")
		node := NewNode("A", func(ctx context.Context, c *TestContext) error {
			val, ok := ctx.Value(myKey).(string)
			assert.True(t, ok)
			assert.Equal(t, expectedValue, val)
			c.Log("A executed")
			return nil
		})
		group.AddNode(node)

		require.NoError(t, group.Build())

		ctx := context.WithValue(context.Background(), myKey, expectedValue)
		err := group.Exec(ctx, testCtx)
		require.NoError(t, err)
		assert.Equal(t, []string{"A executed"}, testCtx.GetResults())
	})
}

func TestConcurrentSafety(t *testing.T) {
	t.Run("concurrent execution with cancellation", func(t *testing.T) {
		testCtx := NewTestContext(10)
		group := NewGroup[*TestContext]("cancellation_group")

		// Add nodes with artificial delays
		for i := 0; i < 10; i++ {
			i := i
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
		logs := testCtx.GetResults()
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
			testCtx := NewTestContext(4)
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

			execOrder := testCtx.GetResults()
			require.Len(t, execOrder, 4)

			// Verify dependency constraints
			aIndex := findIndex(execOrder, "A")
			bIndex := findIndex(execOrder, "B")
			cIndex := findIndex(execOrder, "C")
			dIndex := findIndex(execOrder, "D")

			assert.Less(t, aIndex, bIndex, "A should execute before B")
			assert.Less(t, aIndex, cIndex, "A should execute before C")
			assert.True(t, bIndex < dIndex, "B should execute before D")
			assert.True(t, cIndex < dIndex, "C should execute before D")
		}
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
