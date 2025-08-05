package dagpher

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContext for testing
type TestContext struct {
	mu             sync.Mutex
	log            []string
	executionOrder []string
}

func (c *TestContext) Log(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, msg)
}

func (c *TestContext) GetLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	logCopy := make([]string, len(c.log))
	copy(logCopy, c.log)
	return logCopy
}

func (c *TestContext) AddExecution(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executionOrder = append(c.executionOrder, name)
}

// Tuple2 for calculation tests
type Tuple2 struct {
	First, Second int
}

type Param struct {
	SetDep  bool
	SetName int
	ErrNode []string
}

func Contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if v == vv {
			return true
		}
	}
	return false
}

// Helper function to create calculation nodes
func NewCalcNodes(param Param) (A, B, C, D, E *Node[*Tuple2]) {
	setDep := func(deps ...DependencyNode) []string {
		if param.SetDep {
			var depNames []string
			for _, dep := range deps {
				depNames = append(depNames, dep.Name())
			}
			return depNames
		}
		return nil
	}
	setName := func(name string) string {
		if param.SetName == 0 {
			return name
		}
		return name + strconv.Itoa(param.SetName)
	}
	setErr := func(name string) error {
		if Contains(param.ErrNode, name) {
			return fmt.Errorf("mock error")
		}
		return nil
	}

	A = NewNode(setName("A"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 10)
		if err := setErr("A"); err != nil {
			return err
		}
		c.First += 3
		c.Second += 3
		return nil
	})
	B = NewNode(setName("B"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 20)
		if err := setErr("B"); err != nil {
			return err
		}
		c.First *= 5
		c.Second *= 5
		return nil
	}, setDep(A)...)
	C = NewNode(setName("C"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 200)
		if err := setErr("C"); err != nil {
			return err
		}
		c.Second *= 7
		return nil
	}, setDep(A)...)
	D = NewNode(setName("D"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 300)
		if err := setErr("D"); err != nil {
			return err
		}
		c.First += 11
		return nil
	}, setDep(B)...)
	E = NewNode(setName("E"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 30)
		if err := setErr("E"); err != nil {
			return err
		}
		c.Second += 13
		return nil
	}, setDep(B, C)...)
	return
}

// TestBasicNodeExecution tests basic node functionality
func TestBasicNodeExecution(t *testing.T) {
	t.Run("single node execution", func(t *testing.T) {
		testCtx := &TestContext{}
		node := NewNode("test_node", func(ctx context.Context, c *TestContext) error {
			c.Log("node executed")
			return nil
		})

		graph := NewGraph[*TestContext]()
		graph.AddNode(node)

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		assert.Equal(t, []string{"node executed"}, testCtx.GetLog())
	})

	t.Run("multiple independent nodes", func(t *testing.T) {
		testCtx := &TestContext{}

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		})
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		})

		graph := NewGraph[*TestContext]()
		graph.AddNode(nodeA)
		graph.AddNode(nodeB)
		graph.AddNode(nodeC)

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		logs := testCtx.GetLog()
		assert.Len(t, logs, 3)
		assert.ElementsMatch(t, []string{"A executed", "B executed", "C executed"}, logs)
	})

	t.Run("nodes with dependencies", func(t *testing.T) {
		testCtx := &TestContext{}

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}, "A")
		nodeD := NewNode("D", func(ctx context.Context, c *TestContext) error {
			c.Log("D executed")
			return nil
		}, "B", "C")

		graph := NewGraph[*TestContext]()
		graph.AddNode(nodeA)
		graph.AddNode(nodeB)
		graph.AddNode(nodeC)
		graph.AddNode(nodeD)

		err := graph.Build()
		require.NoError(t, err)

		// Print dependency graph for debugging
		err = graph.PrintDependencyGraph()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		logs := testCtx.GetLog()
		assert.Len(t, logs, 4)

		// Verify execution order
		aIndex := findIndex(logs, "A executed")
		bIndex := findIndex(logs, "B executed")
		cIndex := findIndex(logs, "C executed")
		dIndex := findIndex(logs, "D executed")

		assert.True(t, aIndex < bIndex, "A should execute before B")
		assert.True(t, aIndex < cIndex, "A should execute before C")
		assert.True(t, bIndex < dIndex, "B should execute before D")
		assert.True(t, cIndex < dIndex, "C should execute before D")
	})
}

// TestGroupFunctionality tests Group-related functionality
func TestGroupFunctionality(t *testing.T) {
	t.Run("basic group usage", func(t *testing.T) {
		testCtx := &TestContext{}

		// Create a group
		group := NewGroup[*TestContext]("test_group")

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

		// Create graph and add group
		graph := NewGraph[*TestContext]()
		graph.AddGroup(group)

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		assert.Equal(t, []string{"A executed", "B executed"}, testCtx.GetLog())
	})

	t.Run("multiple groups with cross-group dependencies", func(t *testing.T) {
		testCtx := &TestContext{}

		// Group 1
		group1 := NewGroup[*TestContext]("group1")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A")
		group1.AddNode(nodeA)
		group1.AddNode(nodeB)

		// Group 2
		group2 := NewGroup[*TestContext]("group2")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}, "B") // Cross-group dependency: C depends on B from group1
		nodeD := NewNode("D", func(ctx context.Context, c *TestContext) error {
			c.Log("D executed")
			return nil
		}, "C")
		group2.AddNode(nodeC)
		group2.AddNode(nodeD)

		// Create graph
		graph := NewGraph[*TestContext]()
		graph.AddGroup(group1)
		graph.AddGroup(group2)

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		logs := testCtx.GetLog()
		assert.Len(t, logs, 4)

		// Verify cross-group dependencies
		bIndex := findIndex(logs, "B executed")
		cIndex := findIndex(logs, "C executed")
		assert.True(t, bIndex < cIndex, "B should execute before C (cross-group dependency)")
	})

	t.Run("nested groups", func(t *testing.T) {
		testCtx := &TestContext{}

		// Parent group
		parentGroup := NewGroup[*TestContext]("parent")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		parentGroup.AddNode(nodeA)

		// Child group
		childGroup := NewGroup[*TestContext]("child")
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A") // Depends on node from parent
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}, "B")
		childGroup.AddNode(nodeB)
		childGroup.AddNode(nodeC)

		// Add child to parent
		parentGroup.AddGroup(childGroup)

		// Create graph
		graph := NewGraph[*TestContext]()
		graph.AddGroup(parentGroup)

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		assert.Equal(t, []string{"A executed", "B executed", "C executed"}, testCtx.GetLog())
	})

	t.Run("group entry and exit nodes", func(t *testing.T) {
		group := NewGroup[*TestContext]("test_group")

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error { return nil }, "A")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error { return nil })
		nodeD := NewNode("D", func(ctx context.Context, c *TestContext) error { return nil }, "B", "C")

		group.AddNode(nodeA)
		group.AddNode(nodeB)
		group.AddNode(nodeC)
		group.AddNode(nodeD)

		// Test entry nodes (no internal dependencies)
		entryNodes := group.GetEntryNodes()
		entryNames := make([]string, len(entryNodes))
		for i, node := range entryNodes {
			entryNames[i] = node.Name()
		}
		assert.ElementsMatch(t, []string{"A", "C"}, entryNames)

		// Test exit nodes (no internal dependents)
		exitNodes := group.GetExitNodes()
		exitNames := make([]string, len(exitNodes))
		for i, node := range exitNodes {
			exitNames[i] = node.Name()
		}
		assert.ElementsMatch(t, []string{"D"}, exitNames)
	})
}

// TestDependencyResolver tests the dependency resolution functionality
func TestDependencyResolver(t *testing.T) {
	t.Run("basic dependency resolution", func(t *testing.T) {
		resolver := NewDependencyResolver[*TestContext]()

		group := NewGroup[*TestContext]("test_group")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error { return nil }, "A")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error { return nil }, "B")

		group.AddNode(nodeA)
		group.AddNode(nodeB)
		group.AddNode(nodeC)

		err := resolver.AddGroup(group)
		require.NoError(t, err)

		err = resolver.ResolveDependencies()
		require.NoError(t, err)

		// Test topological order
		order, err := resolver.GetTopologicalOrder()
		require.NoError(t, err)
		assert.Equal(t, []string{"A", "B", "C"}, order)

		// Test dependency queries
		assert.Equal(t, []string{"A"}, resolver.GetNodeDependencies("B"))
		assert.Equal(t, []string{"B"}, resolver.GetNodeDependencies("C"))
		assert.Equal(t, []string{"B"}, resolver.GetNodeDependents("A"))
		assert.Equal(t, []string{"C"}, resolver.GetNodeDependents("B"))
	})

	t.Run("circular dependency detection", func(t *testing.T) {
		resolver := NewDependencyResolver[*TestContext]()

		group := NewGroup[*TestContext]("test_group")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil }, "C")
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error { return nil }, "A")
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error { return nil }, "B")

		group.AddNode(nodeA)
		group.AddNode(nodeB)
		group.AddNode(nodeC)

		err := resolver.AddGroup(group)
		require.NoError(t, err)

		err = resolver.ResolveDependencies()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "circular dependency")
	})

	t.Run("missing dependency detection", func(t *testing.T) {
		resolver := NewDependencyResolver[*TestContext]()

		group := NewGroup[*TestContext]("test_group")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil }, "nonexistent")

		group.AddNode(nodeA)

		err := resolver.AddGroup(group)
		require.NoError(t, err)

		err = resolver.ResolveDependencies()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non-existent")
	})
}

// TestMiddleware tests middleware functionality
func TestMiddleware(t *testing.T) {
	t.Run("middleware execution order", func(t *testing.T) {
		testCtx := &TestContext{}

		mw1 := func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("mw1 start")
				res, err := next(ctx, req)
				testCtx.Log("mw1 end")
				return res, err
			}
		}

		mw2 := func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("mw2 start")
				res, err := next(ctx, req)
				testCtx.Log("mw2 end")
				return res, err
			}
		}

		node := NewNode("test", func(ctx context.Context, c *TestContext) error {
			c.Log("node executed")
			return nil
		})

		graph := NewGraph[*TestContext]()
		graph.AddGlobalMW(mw1)
		graph.AddNode(node, WithMiddlewares(mw2))

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		expectedOrder := []string{
			"mw1 start",
			"mw2 start",
			"node executed",
			"mw2 end",
			"mw1 end",
		}
		assert.Equal(t, expectedOrder, testCtx.GetLog())
	})

	t.Run("retry middleware", func(t *testing.T) {
		testCtx := &TestContext{}
		attempts := 0

		node := NewNode("retry_test", func(ctx context.Context, c *TestContext) error {
			attempts++
			if attempts < 3 {
				return fmt.Errorf("attempt %d failed", attempts)
			}
			c.Log("success on attempt 3")
			return nil
		})

		graph := NewGraph[*TestContext]()
		graph.AddNode(node, WithMiddlewares(RetryMW(3)))

		err := graph.Build()
		require.NoError(t, err)

		err = graph.Exec(context.Background(), testCtx)
		require.NoError(t, err)

		assert.Equal(t, 3, attempts)
		assert.Contains(t, testCtx.GetLog(), "success on attempt 3")
	})
}

// TestCalculationWorkflow tests the complex calculation workflow
func TestCalculationWorkflow(t *testing.T) {
	ctx := context.Background()

	Convey("Test calculation workflow", t, func() {
		Convey("serial execution", func() {
			A, B, C, D, E := NewCalcNodes(Param{SetDep: true})
			graph := NewGraph[*Tuple2]().SetMaxGoNum(1)
			now := time.Now()

			graph.AddNode(A)
			graph.AddNode(B)
			graph.AddNode(C)
			graph.AddNode(D)
			graph.AddNode(E)

			exeCtx := &Tuple2{First: 1, Second: 1}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldBeNil)
			So(exeCtx.First, ShouldEqual, 31)
			So(exeCtx.Second, ShouldEqual, 153)

			cost := time.Since(now)
			So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*560)
		})

		Convey("parallel execution", func() {
			A, B, C, D, E := NewCalcNodes(Param{SetDep: true})
			graph := NewGraph[*Tuple2]()
			now := time.Now()

			graph.AddNode(A)
			graph.AddNode(B)
			graph.AddNode(C)
			graph.AddNode(D)
			graph.AddNode(E)

			exeCtx := &Tuple2{First: 1, Second: 1}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldBeNil)
			So(exeCtx.First, ShouldEqual, 31)
			So(exeCtx.Second, ShouldEqual, 153)

			cost := time.Since(now)
			So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*330)
			So(cost, ShouldBeLessThan, time.Millisecond*350)
		})

		Convey("error handling", func() {
			A, B, C, D, E := NewCalcNodes(Param{SetDep: true, ErrNode: []string{"C"}})
			graph := NewGraph[*Tuple2]()

			graph.AddNode(A)
			graph.AddNode(B)
			graph.AddNode(C)
			graph.AddNode(D)
			graph.AddNode(E)

			exeCtx := &Tuple2{First: 1, Second: 1}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldNotBeNil)

			// D should still execute (depends only on B)
			So(exeCtx.First, ShouldEqual, 31)
			// E should not execute (depends on C which failed)
			So(exeCtx.Second, ShouldEqual, 20) // Only A and B executed
		})
	})
}

// TestConcurrencyControl tests semaphore-based concurrency control
func TestConcurrencyControl(t *testing.T) {
	t.Run("serial execution with maxGoNum=1", func(t *testing.T) {
		var execOrder []string
		var orderMutex sync.Mutex

		addToOrder := func(name string) {
			orderMutex.Lock()
			defer orderMutex.Unlock()
			execOrder = append(execOrder, name)
		}

		createNode := func(name string) *Node[*TestContext] {
			return NewNode(name, func(ctx context.Context, c *TestContext) error {
				addToOrder(name + "_start")
				time.Sleep(50 * time.Millisecond)
				addToOrder(name + "_end")
				return nil
			})
		}

		nodeA := createNode("A")
		nodeB := createNode("B")
		nodeC := createNode("C")

		graph := NewGraph[*TestContext]()
		graph.SetMaxGoNum(1) // Force serial execution
		graph.AddNode(nodeA)
		graph.AddNode(nodeB)
		graph.AddNode(nodeC)

		start := time.Now()
		err := graph.Exec(context.Background(), &TestContext{})
		duration := time.Since(start)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, duration, 150*time.Millisecond) // 3 * 50ms

		// Verify serial execution
		orderMutex.Lock()
		defer orderMutex.Unlock()
		assert.Len(t, execOrder, 6)

		// Check that each node completes before the next starts
		for i := 0; i < len(execOrder)-1; i += 2 {
			startEvent := execOrder[i]
			endEvent := execOrder[i+1]
			assert.True(t, len(startEvent) > 6 && startEvent[len(startEvent)-6:] == "_start")
			assert.True(t, len(endEvent) > 4 && endEvent[len(endEvent)-4:] == "_end")
		}
	})

	t.Run("limited parallel execution", func(t *testing.T) {
		var activeCount int32
		var maxActiveCount int32

		createNode := func(name string) *Node[*TestContext] {
			return NewNode(name, func(ctx context.Context, c *TestContext) error {
				current := atomic.AddInt32(&activeCount, 1)
				for {
					max := atomic.LoadInt32(&maxActiveCount)
					if current <= max || atomic.CompareAndSwapInt32(&maxActiveCount, max, current) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt32(&activeCount, -1)
				return nil
			})
		}

		nodeA := createNode("A")
		nodeB := createNode("B")
		nodeC := createNode("C")
		nodeD := createNode("D")

		graph := NewGraph[*TestContext]()
		graph.SetMaxGoNum(2) // Allow max 2 concurrent executions
		graph.AddNode(nodeA)
		graph.AddNode(nodeB)
		graph.AddNode(nodeC)
		graph.AddNode(nodeD)

		start := time.Now()
		err := graph.Exec(context.Background(), &TestContext{})
		duration := time.Since(start)

		require.NoError(t, err)
		// Should take around 200ms (2 batches of 2 parallel executions)
		assert.GreaterOrEqual(t, duration, 190*time.Millisecond)
		assert.Less(t, duration, 250*time.Millisecond)

		// Verify max concurrent executions never exceeded 2
		assert.LessOrEqual(t, atomic.LoadInt32(&maxActiveCount), int32(2))
	})
}

// Utility function to find index of element in slice
func findIndex(slice []string, element string) int {
	for i, v := range slice {
		if v == element {
			return i
		}
	}
	return -1
}
