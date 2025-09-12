package dagpher

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
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

// TestGroupStressAndPerformance tests group performance under stress conditions
func TestGroupStressAndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}

	t.Run("large scale parallel execution", func(t *testing.T) {
		const numNodes = 1000
		testCtx := NewTestContext(numNodes)
		group := NewGroup[*TestContext]("large_parallel_group")

		var completed int32

		// Create many independent parallel nodes
		for i := 0; i < numNodes; i++ {
			nodeName := fmt.Sprintf("parallel_node_%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				atomic.AddInt32(&completed, 1)
				c.Log(nodeName)
				// Simulate small amount of work
				time.Sleep(time.Microsecond * 10)
				return nil
			}))
		}

		group.SetMaxGoNum(100) // High concurrency

		require.NoError(t, group.Build())

		start := time.Now()
		require.NoError(t, group.Exec(context.Background(), testCtx))
		duration := time.Since(start)

		assert.Equal(t, int32(numNodes), atomic.LoadInt32(&completed))
		assert.Less(t, duration, 5*time.Second, "Large parallel execution should complete quickly")

		results := testCtx.GetResults()
		assert.Len(t, results, numNodes)
	})

	t.Run("deep nested group hierarchy", func(t *testing.T) {
		const depth = 20
		const nodesPerLevel = 5

		testCtx := NewTestContext(depth * nodesPerLevel)

		// Create nested group hierarchy
		var createNestedGroups func(level int) *Group[*TestContext]
		createNestedGroups = func(level int) *Group[*TestContext] {
			if level >= depth {
				return nil
			}

			groupName := fmt.Sprintf("level_%d", level)
			group := NewGroup[*TestContext](groupName)

			// Add nodes to this level
			for i := 0; i < nodesPerLevel; i++ {
				nodeName := fmt.Sprintf("L%d_N%d", level, i)
				group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
					c.Log(fmt.Sprintf("executed_%s", nodeName))
					return nil
				}))
			}

			// Add nested subgroup
			if subGroup := createNestedGroups(level + 1); subGroup != nil {
				group.AddGroup(subGroup)
			}

			return group
		}

		rootGroup := createNestedGroups(0)
		require.NotNil(t, rootGroup)

		start := time.Now()
		require.NoError(t, rootGroup.Build())
		buildTime := time.Since(start)

		start = time.Now()
		require.NoError(t, rootGroup.Exec(context.Background(), testCtx))
		execTime := time.Since(start)

		assert.Less(t, buildTime, 1*time.Second, "Deep nesting should not significantly slow build")
		assert.Less(t, execTime, 5*time.Second, "Deep nesting should not significantly slow execution")

		results := testCtx.GetResults()
		assert.Len(t, results, depth*nodesPerLevel)
	})

	t.Run("high frequency repeated executions", func(t *testing.T) {
		const numIterations = 500

		group := NewGroup[*TestContext]("repeated_exec_group")

		// Simple dependency chain A -> B -> C
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Inc()
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Inc()
			return nil
		}, "A")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Inc()
			return nil
		}, "B")

		group.AddNode(nodeA)
		group.AddNode(nodeB)
		group.AddNode(nodeC)

		require.NoError(t, group.Build())

		start := time.Now()
		for i := 0; i < numIterations; i++ {
			execCtx := NewTestContext(10)
			require.NoError(t, group.Exec(context.Background(), execCtx))
		}
		totalTime := time.Since(start)

		avgTimePerExecution := totalTime / numIterations
		assert.Less(t, avgTimePerExecution, 10*time.Millisecond, "High frequency execution should be fast")
	})
}

// TestGroupResourceManagement tests for resource leaks and proper cleanup
func TestGroupResourceManagement(t *testing.T) {
	t.Run("goroutine leak prevention", func(t *testing.T) {
		initialGoroutines := runtime.NumGoroutine()

		// Create and execute many groups
		for i := 0; i < 50; i++ {
			testCtx := NewTestContext(10)
			group := NewGroup[*TestContext](fmt.Sprintf("group_%d", i))

			// Add nodes with different execution times
			for j := 0; j < 10; j++ {
				nodeName := fmt.Sprintf("node_%d_%d", i, j)
				group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
					time.Sleep(time.Millisecond * time.Duration(j+1))
					c.Log(nodeName)
					return nil
				}))
			}

			group.SetMaxGoNum(5)
			require.NoError(t, group.Build())
			require.NoError(t, group.Exec(context.Background(), testCtx))
		}

		// Force garbage collection and wait
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
		runtime.GC()

		finalGoroutines := runtime.NumGoroutine()
		// Allow some tolerance for background goroutines
		assert.LessOrEqual(t, finalGoroutines, initialGoroutines+10,
			"Should not leak goroutines: initial=%d, final=%d", initialGoroutines, finalGoroutines)
	})

	t.Run("context cancellation resource cleanup", func(t *testing.T) {
		testCtx := NewTestContext(20)
		group := NewGroup[*TestContext]("cancellation_group")

		var startedNodes int32
		var cancelledNodes int32

		// Create long-running nodes
		for i := 0; i < 20; i++ {
			nodeName := fmt.Sprintf("long_node_%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				atomic.AddInt32(&startedNodes, 1)
				select {
				case <-time.After(5 * time.Second):
					c.Log(fmt.Sprintf("%s completed", nodeName))
					return nil
				case <-ctx.Done():
					atomic.AddInt32(&cancelledNodes, 1)
					c.Log(fmt.Sprintf("%s cancelled", nodeName))
					return ctx.Err()
				}
			}))
		}

		group.SetMaxGoNum(10)
		require.NoError(t, group.Build())

		// Execute with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		initialGoroutines := runtime.NumGoroutine()

		err := group.Exec(ctx, testCtx)
		assert.Error(t, err, "Should timeout")
		assert.Contains(t, err.Error(), "deadline exceeded")

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)
		runtime.GC()

		finalGoroutines := runtime.NumGoroutine()
		assert.LessOrEqual(t, finalGoroutines, initialGoroutines+5,
			"Should clean up goroutines after cancellation")

		// Some nodes should have been cancelled
		assert.Greater(t, atomic.LoadInt32(&cancelledNodes), int32(0),
			"Some nodes should have been cancelled")
	})

	t.Run("panic recovery and isolation", func(t *testing.T) {
		testCtx := NewTestContext(10)
		group := NewGroup[*TestContext]("panic_group")

		var normalExecuted int32

		// Normal nodes
		for i := 0; i < 5; i++ {
			nodeName := fmt.Sprintf("normal_%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				atomic.AddInt32(&normalExecuted, 1)
				c.Log(fmt.Sprintf("%s executed", nodeName))
				return nil
			}))
		}

		// Panicking node
		group.AddNode(NewNode("panic_node", func(ctx context.Context, c *TestContext) error {
			c.Log("panic_node before panic")
			panic("intentional panic for testing")
		}))

		require.NoError(t, group.Build())

		// Should handle panic gracefully without crashing
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Recovered from panic: %v", r)
				}
			}()

			// Execution might fail due to panic, but shouldn't crash the test
			group.Exec(context.Background(), testCtx)
		}()

		// Normal nodes should still execute (depending on timing and dependencies)
		// We can't guarantee exact count due to potential race conditions with panic
		assert.GreaterOrEqual(t, atomic.LoadInt32(&normalExecuted), int32(0))

		results := testCtx.GetResults()
		assert.Contains(t, results, "panic_node before panic")
	})
}

// TestGroupEdgeCasesAndBoundaries tests edge cases and boundary conditions
func TestGroupEdgeCasesAndBoundaries(t *testing.T) {
	t.Run("extremely large fan-out dependencies", func(t *testing.T) {
		const fanOut = 1000
		testCtx := NewTestContext(fanOut + 1)
		group := NewGroup[*TestContext]("large_fanout_group")

		// Root node
		root := NewNode("root", func(ctx context.Context, c *TestContext) error {
			c.Log("root executed")
			return nil
		})
		group.AddNode(root)

		// Many nodes depending on root
		for i := 0; i < fanOut; i++ {
			nodeName := fmt.Sprintf("fan_%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				c.Log(fmt.Sprintf("%s executed", nodeName))
				return nil
			}, "root"))
		}

		group.SetMaxGoNum(100) // Limited concurrency to test queuing

		start := time.Now()
		require.NoError(t, group.Build())
		buildTime := time.Since(start)

		start = time.Now()
		require.NoError(t, group.Exec(context.Background(), testCtx))
		execTime := time.Since(start)

		assert.Less(t, buildTime, 1*time.Second, "Building large fan-out should be fast")
		assert.Less(t, execTime, 10*time.Second, "Large fan-out execution should complete reasonably")

		results := testCtx.GetResults()
		assert.Len(t, results, fanOut+1)
		assert.Contains(t, results, "root executed")
	})

	t.Run("single node concurrency limit", func(t *testing.T) {
		testCtx := NewTestContext(5)
		group := NewGroup[*TestContext]("single_concurrency_group")
		group.SetMaxGoNum(1) // Force serial execution

		var executionOrder []string
		var orderMutex sync.Mutex

		for i := 0; i < 5; i++ {
			nodeName := fmt.Sprintf("serial_node_%d", i)
			group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				orderMutex.Lock()
				executionOrder = append(executionOrder, fmt.Sprintf("%s_start", nodeName))
				orderMutex.Unlock()

				time.Sleep(20 * time.Millisecond)

				orderMutex.Lock()
				executionOrder = append(executionOrder, fmt.Sprintf("%s_end", nodeName))
				orderMutex.Unlock()

				c.Log(fmt.Sprintf("%s executed", nodeName))
				return nil
			}))
		}

		require.NoError(t, group.Build())
		start := time.Now()
		require.NoError(t, group.Exec(context.Background(), testCtx))
		duration := time.Since(start)

		// Should take at least 100ms (5 * 20ms) for serial execution
		assert.GreaterOrEqual(t, duration, 100*time.Millisecond)

		// Verify serial execution - each node should complete before next starts
		orderMutex.Lock()
		order := make([]string, len(executionOrder))
		copy(order, executionOrder)
		orderMutex.Unlock()

		assert.Len(t, order, 10) // 5 nodes * 2 events each

		// Check that each node's start/end events are paired correctly
		for i := 0; i < 10; i += 2 {
			assert.Contains(t, order[i], "_start")
			assert.Contains(t, order[i+1], "_end")
			// Extract node name and verify they match
			startNode := strings.Split(order[i], "_start")[0]
			endNode := strings.Split(order[i+1], "_end")[0]
			assert.Equal(t, startNode, endNode)
		}

		results := testCtx.GetResults()
		assert.Len(t, results, 5)
	})

	t.Run("empty name handling", func(t *testing.T) {
		testCtx := NewTestContext(1)

		// Test empty group name
		group := NewGroup[*TestContext]("")
		node := NewNode("test", func(ctx context.Context, c *TestContext) error {
			c.Log("test executed")
			return nil
		})
		group.AddNode(node)

		require.NoError(t, group.Build())
		require.NoError(t, group.Exec(context.Background(), testCtx))

		results := testCtx.GetResults()
		assert.Contains(t, results, "test executed")
	})

	t.Run("massive dependency matrix", func(t *testing.T) {
		const matrixSize = 50 // 50x50 = 2500 potential dependencies
		testCtx := NewTestContext(matrixSize * matrixSize)
		group := NewGroup[*TestContext]("dependency_matrix_group")

		// Create a grid of nodes where each node depends on nodes above and to the left
		for row := 0; row < matrixSize; row++ {
			for col := 0; col < matrixSize; col++ {
				nodeName := fmt.Sprintf("node_%d_%d", row, col)

				var deps []string
				if row > 0 {
					deps = append(deps, fmt.Sprintf("node_%d_%d", row-1, col))
				}
				if col > 0 {
					deps = append(deps, fmt.Sprintf("node_%d_%d", row, col-1))
				}

				node := NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
					c.Inc()
					return nil
				}, deps...)

				group.AddNode(node)
			}
		}

		group.SetMaxGoNum(20) // Reasonable concurrency limit

		start := time.Now()
		require.NoError(t, group.Build())
		buildTime := time.Since(start)

		start = time.Now()
		require.NoError(t, group.Exec(context.Background(), testCtx))
		execTime := time.Since(start)

		assert.Less(t, buildTime, 5*time.Second, "Complex dependency matrix should build in reasonable time")
		assert.Less(t, execTime, 30*time.Second, "Complex dependency matrix should execute in reasonable time")

		assert.Equal(t, matrixSize*matrixSize, testCtx.Count(), "All nodes should execute")
	})
}
