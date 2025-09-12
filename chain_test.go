package dagpher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestChainBasicExecution(t *testing.T) {
	ctx := context.Background()
	testCtx := NewTestContext(3)

	// Create chain
	chain := NewChain[*TestContext]()

	// Add nodes in order
	chain.AddNode(NewNode("node1", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("node1")
		return nil
	})).
		AddNode(NewNode("node2", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("node2")
			return nil
		})).
		AddNode(NewNode("node3", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("node3")
			return nil
		}))

	// Build and execute
	if err := chain.Build(); err != nil {
		t.Fatalf("Failed to build chain: %v", err)
	}

	if err := chain.Exec(ctx, testCtx); err != nil {
		t.Fatalf("Failed to execute chain: %v", err)
	}

	// Verify execution order
	expected := []string{"node1", "node2", "node3"}
	results := testCtx.GetResults()
	if len(results) != len(expected) {
		t.Fatalf("Expected %d executions, got %d", len(expected), len(results))
	}

	for i, expectedName := range expected {
		if results[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, results[i])
		}
	}
}

func TestChainWithGroups(t *testing.T) {
	ctx := context.Background()
	testCtx := NewTestContext(4)

	// Create a group
	group := NewGroup[*TestContext]("test-group")
	group.AddNode(NewNode("group-node1", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("group-node1")
		return nil
	}))
	group.AddNode(NewNode("group-node2", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("group-node2")
		return nil
	}))

	// Create chain with mixed nodes and groups
	chain := NewChain[*TestContext]()
	chain.AddNode(NewNode("before-group", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("before-group")
		return nil
	})).
		AddNode(group.AsNode()).
		AddNode(NewNode("after-group", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("after-group")
			return nil
		}))
	chain.AddGlobalMW(LoggerMW())

	// Build and execute
	if err := chain.Build(); err != nil {
		t.Fatalf("Failed to build chain: %v", err)
	}

	if err := chain.Exec(ctx, testCtx); err != nil {
		t.Fatalf("Failed to execute chain: %v", err)
	}

	results := testCtx.GetResults()
	// Verify that before-group executed first and after-group executed last
	if len(results) < 3 {
		t.Fatalf("Expected at least 3 executions, got %d", len(results))
	}

	if results[0] != "before-group" {
		t.Errorf("Expected first execution to be 'before-group', got %s", results[0])
	}

	lastIndex := len(results) - 1
	if results[lastIndex] != "after-group" {
		t.Errorf("Expected last execution to be 'after-group', got %s", results[lastIndex])
	}
}

func TestChainWithMiddleware(t *testing.T) {
	ctx := context.Background()
	testCtx := NewTestContext(3)

	// Create middleware that adds prefix
	prefixMW := func() Middleware {
		return func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				// Add prefix to execution
				if tc, ok := req.(*TestContext); ok {
					tc.AddExecution("mw-before-" + node.Name())
				}

				result, err := next(ctx, req)

				// Add suffix to execution
				if tc, ok := req.(*TestContext); ok {
					tc.AddExecution("mw-after-" + node.Name())
				}

				return result, err
			}
		}
	}

	// Create chain with middleware
	chain := NewChain[*TestContext]()
	chain.AddGlobalMW(prefixMW()).
		AddNode(NewNode("test-node", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("test-node")
			return nil
		}))

	// Build and execute
	if err := chain.Build(); err != nil {
		t.Fatalf("Failed to build chain: %v", err)
	}

	if err := chain.Exec(ctx, testCtx); err != nil {
		t.Fatalf("Failed to execute chain: %v", err)
	}

	// Verify middleware execution
	expected := []string{"mw-before-test-node", "test-node", "mw-after-test-node"}
	results := testCtx.GetResults()
	if len(results) != len(expected) {
		t.Fatalf("Expected %d executions, got %d: %v", len(expected), len(results), results)
	}

	for i, expectedName := range expected {
		if results[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, results[i])
		}
	}
}

func TestChainConcurrencyControl(t *testing.T) {
	ctx := context.Background()
	testCtx := NewTestContext(6)

	// Create chain with maxGoNum=1 (serial execution)
	chain := NewChain[*TestContext]()
	chain.SetMaxGoNum(1)

	// Add nodes with delays to test serialization
	for i := 0; i < 3; i++ {
		nodeName := fmt.Sprintf("node%d", i+1)
		chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
			c.AddExecution(nodeName + "-start")
			time.Sleep(10 * time.Millisecond) // Small delay
			c.AddExecution(nodeName + "-end")
			return nil
		}))
	}

	// Build and execute
	if err := chain.Build(); err != nil {
		t.Fatalf("Failed to build chain: %v", err)
	}

	start := time.Now()
	if err := chain.Exec(ctx, testCtx); err != nil {
		t.Fatalf("Failed to execute chain: %v", err)
	}
	duration := time.Since(start)

	// Verify serial execution (should take at least 30ms due to delays)
	if duration < 30*time.Millisecond {
		t.Errorf("Expected execution to take at least 30ms, took %v", duration)
	}

	results := testCtx.GetResults()
	// Verify execution order is maintained
	if len(results) != 6 {
		t.Fatalf("Expected 6 executions, got %d: %v", len(results), results)
	}
}

func TestChainErrorHandling(t *testing.T) {
	ctx := context.Background()
	testCtx := NewTestContext(2)

	// Create chain with a failing node
	chain := NewChain[*TestContext]()
	chain.AddNode(NewNode("node1", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("node1")
		return nil
	})).
		AddNode(NewNode("failing-node", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("failing-node")
			return fmt.Errorf("intentional error")
		})).
		AddNode(NewNode("node3", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("node3")
			return nil
		}))

	// Build and execute
	if err := chain.Build(); err != nil {
		t.Fatalf("Failed to build chain: %v", err)
	}

	err := chain.Exec(ctx, testCtx)
	if err == nil {
		t.Fatal("Expected chain execution to fail, but it succeeded")
	}

	// Verify that execution stopped at the failing node
	expected := []string{"node1", "failing-node"}
	results := testCtx.GetResults()
	if len(results) != len(expected) {
		t.Fatalf("Expected %d executions, got %d: %v", len(expected), len(results), results)
	}

	for i, expectedName := range expected {
		if results[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, results[i])
		}
	}
}

func TestChainPipeline(t *testing.T) {
	ctx := context.Background()

	Convey("serial pipeline", t, func() {
		A, B, C, D, E := NewCalcNodes(Param{SetDep: false}) // Dependencies are ignored in chain
		chain := NewChain[*Tuple2]().SetMaxGoNum(1)

		chain.AddNode(A)
		chain.AddNode(B)
		chain.AddNode(C)
		chain.AddNode(D)
		chain.AddNode(E)

		exeCtx := &Tuple2{
			First:  1,
			Second: 1,
		}

		now := time.Now()
		err := chain.Exec(ctx, exeCtx)
		cost := time.Since(now)

		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 153)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*560)
		So(cost, ShouldBeLessThan, time.Millisecond*569) // Add a little buffer
	})

	Convey("parallel pipeline with group", t, func() {
		A, B, C, D, E := NewCalcNodes(Param{SetDep: false}) // Dependencies needed for group
		chain := NewChain[*Tuple2]()

		// Group B and C to run in parallel
		groupBC := NewGroup[*Tuple2]("group-bc")
		groupBC.AddNode(B)
		groupBC.AddNode(C)

		// Build the chain A -> Group(B,C) -> E -> D
		// Note: D depends on B, E depends on B and C.
		// To make the chain valid, we need to ensure dependencies are met.
		// A -> groupBC(B,C) -> E -> D is a valid sequence.
		chain.AddNode(A)
		chain.AddNode(groupBC.AsNode())
		chain.AddNode(E)
		chain.AddNode(D)

		ctx, graphviz := newGraphvizBuilder("flow").Build(ctx)
		defer graphviz.Log(ctx)
		chain.AddGlobalMW(GraphvizMW())

		exeCtx := &Tuple2{
			First:  1,
			Second: 1,
		}

		now := time.Now()
		err := chain.Exec(ctx, exeCtx)
		cost := time.Since(now)

		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 153)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*540)
		So(cost, ShouldBeLessThan, time.Millisecond*549) // Add a little buffer
	})
}

type ChainExecContext struct {
	Data    string
	Counter int
}

func TestChainExecPath(t *testing.T) {
	// 创建 Chain
	chain := NewChain[*ChainExecContext]()

	// 添加全局中间件
	chain.AddGlobalMW(LoggerMW())

	// 创建一些普通节点
	node1 := NewNode("node1", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> Node1 executing in path: %s\n", path.String())
		}
		c.Counter++
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// 创建组节点
	group1 := NewGroup[*ChainExecContext]("group1")
	group1.AddNode(NewNode("A1", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> A1 executing in path: %s\n", path.String())
		}
		c.Counter += 10
		time.Sleep(10 * time.Millisecond)
		return nil
	}))

	group1.AddNode(NewNode("B1", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> B1 executing in path: %s\n", path.String())
		}
		c.Counter += 100
		time.Sleep(10 * time.Millisecond)
		return nil
	}))

	// 创建嵌套组
	group2 := NewGroup[*ChainExecContext]("group2")
	subGroup := NewGroup[*ChainExecContext]("subgroup")
	subGroup.AddNode(NewNode("S1", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> S1 executing in path: %s\n", path.String())
		}
		c.Counter += 1000
		time.Sleep(10 * time.Millisecond)
		return nil
	}))

	group2.AddNode(NewNode("A2", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> A2 executing in path: %s\n", path.String())
		}
		c.Counter += 10000
		time.Sleep(10 * time.Millisecond)
		return nil
	}))
	group2.AddGroup(subGroup)

	// 添加节点到 Chain（按顺序执行）
	chain.AddNode(node1)
	chain.AddNode(group1.AsNode())
	chain.AddNode(group2.AsNode())

	// 构建和执行
	if err := chain.Build(); err != nil {
		panic(err)
	}

	execCtx := &ChainExecContext{Data: "test", Counter: 0}
	ctx := context.Background()

	fmt.Println("=== Starting Chain execution ===")
	fmt.Println("Expected execution order and paths:")
	fmt.Println("  1. node1: chain")
	fmt.Println("  2. group1 nodes (A1, B1): chain/group1")
	fmt.Println("  3. group2 nodes:")
	fmt.Println("     - A2: chain/group2")
	fmt.Println("     - S1: chain/group2/subgroup")
	fmt.Println()

	if err := chain.Exec(ctx, execCtx); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	fmt.Printf("Final counter value: %d\n", execCtx.Counter)
	fmt.Println("=== Chain execution completed ===")

	// 测试独立的 Chain（不在任何父容器中）
	fmt.Println("\n=== Standalone Chain execution ===")
	standaloneChain := NewChain[*ChainExecContext]()
	standaloneChain.AddGlobalMW(LoggerMW())

	standaloneNode := NewNode("standalone", func(ctx context.Context, c *ChainExecContext) error {
		if path, ok := GetHierarchyPath(ctx); ok {
			fmt.Printf("  -> Standalone node executing in path: %s\n", path.String())
		} else {
			fmt.Printf("  -> Standalone node: no hierarchy path\n")
		}
		return nil
	})

	standaloneChain.AddNode(standaloneNode)

	if err := standaloneChain.Build(); err != nil {
		panic(err)
	}

	standaloneCtx := &ChainExecContext{Data: "standalone"}
	if err := standaloneChain.Exec(context.Background(), standaloneCtx); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// TestChainStressAndPerformance tests chain performance under stress conditions
func TestChainStressAndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}

	t.Run("long chain execution performance", func(t *testing.T) {
		const chainLength = 500
		testCtx := NewTestContext(chainLength)
		chain := NewChain[*TestContext]()

		// Create a long chain
		for i := 0; i < chainLength; i++ {
			nodeName := fmt.Sprintf("chain_node_%d", i)
			chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				c.Log(nodeName)
				// Minimal work to test overhead
				time.Sleep(time.Microsecond * 10)
				return nil
			}))
		}

		chain.SetMaxGoNum(1) // Serial execution for chain semantics

		start := time.Now()
		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build long chain: %v", err)
		}
		buildTime := time.Since(start)

		start = time.Now()
		if err := chain.Exec(context.Background(), testCtx); err != nil {
			t.Fatalf("Failed to execute long chain: %v", err)
		}
		execTime := time.Since(start)

		if buildTime > 1*time.Second {
			t.Errorf("Building long chain took too long: %v", buildTime)
		}

		if execTime > 10*time.Second {
			t.Errorf("Executing long chain took too long: %v", execTime)
		}

		results := testCtx.GetResults()
		if len(results) != chainLength {
			t.Errorf("Expected %d executions, got %d", chainLength, len(results))
		}

		// Verify execution order
		for i, result := range results {
			expectedName := fmt.Sprintf("chain_node_%d", i)
			if result != expectedName {
				t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, result)
			}
		}
	})

	t.Run("high frequency chain execution", func(t *testing.T) {
		const numIterations = 200

		chain := NewChain[*TestContext]()

		// Simple 3-node chain
		chain.AddNode(NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Inc()
			return nil
		})).
			AddNode(NewNode("B", func(ctx context.Context, c *TestContext) error {
				c.Inc()
				return nil
			})).
			AddNode(NewNode("C", func(ctx context.Context, c *TestContext) error {
				c.Inc()
				return nil
			}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain: %v", err)
		}

		start := time.Now()
		for i := 0; i < numIterations; i++ {
			execCtx := NewTestContext(10)
			if err := chain.Exec(context.Background(), execCtx); err != nil {
				t.Fatalf("Failed to execute chain iteration %d: %v", i, err)
			}
		}
		totalTime := time.Since(start)

		avgTime := totalTime / numIterations
		if avgTime > 5*time.Millisecond {
			t.Errorf("High frequency execution too slow: avg %v per iteration", avgTime)
		}
		t.Logf("Average execution time: %v", avgTime)
	})

	t.Run("chain with large groups", func(t *testing.T) {
		const groupSize = 100
		const numGroups = 5

		testCtx := NewTestContext(groupSize * numGroups)
		chain := NewChain[*TestContext]()

		// Create chain with alternating groups
		for g := 0; g < numGroups; g++ {
			group := NewGroup[*TestContext](fmt.Sprintf("large_group_%d", g))

			for i := 0; i < groupSize; i++ {
				nodeName := fmt.Sprintf("G%d_N%d", g, i)
				group.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
					c.Log(nodeName)
					return nil
				}))
			}

			chain.AddNode(group.AsNode())
		}

		chain.SetMaxGoNum(20) // Allow some parallelism within groups

		start := time.Now()
		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain with large groups: %v", err)
		}
		buildTime := time.Since(start)

		start = time.Now()
		if err := chain.Exec(context.Background(), testCtx); err != nil {
			t.Fatalf("Failed to execute chain with large groups: %v", err)
		}
		execTime := time.Since(start)

		if buildTime > 2*time.Second {
			t.Errorf("Building chain with large groups took too long: %v", buildTime)
		}

		if execTime > 15*time.Second {
			t.Errorf("Executing chain with large groups took too long: %v", execTime)
		}

		results := testCtx.GetResults()
		if len(results) != groupSize*numGroups {
			t.Errorf("Expected %d total executions, got %d", groupSize*numGroups, len(results))
		}
	})
}

// TestChainResourceManagement tests for resource leaks and proper cleanup
func TestChainResourceManagement(t *testing.T) {
	t.Run("goroutine leak prevention", func(t *testing.T) {
		initialGoroutines := runtime.NumGoroutine()

		// Create and execute many chains
		for i := 0; i < 50; i++ {
			testCtx := NewTestContext(5)
			chain := NewChain[*TestContext]()

			// Add nodes with varying execution times
			for j := 0; j < 5; j++ {
				nodeName := fmt.Sprintf("chain_%d_node_%d", i, j)
				chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
					time.Sleep(time.Millisecond * time.Duration(j+1))
					c.Log(nodeName)
					return nil
				}))
			}

			if err := chain.Build(); err != nil {
				t.Fatalf("Failed to build chain %d: %v", i, err)
			}

			if err := chain.Exec(context.Background(), testCtx); err != nil {
				t.Fatalf("Failed to execute chain %d: %v", i, err)
			}
		}

		// Force garbage collection and wait
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
		runtime.GC()

		finalGoroutines := runtime.NumGoroutine()
		// Allow some tolerance for background goroutines
		if finalGoroutines > initialGoroutines+10 {
			t.Errorf("Potential goroutine leak: initial=%d, final=%d", initialGoroutines, finalGoroutines)
		}
	})

	t.Run("context cancellation cleanup", func(t *testing.T) {
		testCtx := NewTestContext(10)
		chain := NewChain[*TestContext]()

		var startedNodes int32
		var cancelledNodes int32

		// Create nodes that can detect cancellation
		for i := 0; i < 10; i++ {
			nodeName := fmt.Sprintf("cancellable_node_%d", i)
			chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				atomic.AddInt32(&startedNodes, 1)
				select {
				case <-time.After(2 * time.Second):
					c.Log(fmt.Sprintf("%s completed", nodeName))
					return nil
				case <-ctx.Done():
					atomic.AddInt32(&cancelledNodes, 1)
					c.Log(fmt.Sprintf("%s cancelled", nodeName))
					return ctx.Err()
				}
			}))
		}

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain: %v", err)
		}

		// Execute with short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		initialGoroutines := runtime.NumGoroutine()

		err := chain.Exec(ctx, testCtx)
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Errorf("Expected timeout error, got: %v", err)
		}

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)
		runtime.GC()

		finalGoroutines := runtime.NumGoroutine()
		if finalGoroutines > initialGoroutines+5 {
			t.Errorf("Goroutines not cleaned up after cancellation: initial=%d, final=%d",
				initialGoroutines, finalGoroutines)
		}

		// Should have cancelled some nodes
		if atomic.LoadInt32(&cancelledNodes) == 0 {
			t.Error("Expected some nodes to be cancelled, but none were")
		}
	})

	t.Run("panic recovery and isolation", func(t *testing.T) {
		testCtx := NewTestContext(5)
		chain := NewChain[*TestContext]()

		var normalExecuted int32

		// Add normal nodes
		chain.AddNode(NewNode("before_panic", func(ctx context.Context, c *TestContext) error {
			atomic.AddInt32(&normalExecuted, 1)
			c.Log("before_panic executed")
			return nil
		}))

		// Add panicking node
		chain.AddNode(NewNode("panic_node", func(ctx context.Context, c *TestContext) error {
			c.Log("panic_node about to panic")
			panic("intentional panic for testing")
		}))

		// This should not execute due to panic
		chain.AddNode(NewNode("after_panic", func(ctx context.Context, c *TestContext) error {
			atomic.AddInt32(&normalExecuted, 1)
			c.Log("after_panic executed")
			return nil
		}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain: %v", err)
		}

		// Should handle panic gracefully
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Recovered from panic: %v", r)
				}
			}()
			chain.Exec(context.Background(), testCtx)
		}()

		// First node should execute
		if atomic.LoadInt32(&normalExecuted) == 0 {
			t.Error("Expected at least one normal node to execute")
		}

		results := testCtx.GetResults()
		if !contains(results, "before_panic executed") {
			t.Error("Node before panic should have executed")
		}
		if !contains(results, "panic_node about to panic") {
			t.Error("Panic node should have started execution")
		}
	})
}

// TestChainEdgeCasesAndBoundaries tests edge cases and boundary conditions
func TestChainEdgeCasesAndBoundaries(t *testing.T) {
	t.Run("empty chain execution", func(t *testing.T) {
		testCtx := NewTestContext(1)
		chain := NewChain[*TestContext]()

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build empty chain: %v", err)
		}

		start := time.Now()
		if err := chain.Exec(context.Background(), testCtx); err != nil {
			t.Fatalf("Failed to execute empty chain: %v", err)
		}
		duration := time.Since(start)

		// Should complete very quickly
		if duration > 100*time.Millisecond {
			t.Errorf("Empty chain took too long: %v", duration)
		}

		results := testCtx.GetResults()
		if len(results) != 0 {
			t.Errorf("Expected no executions for empty chain, got %d", len(results))
		}
	})

	t.Run("single node chain", func(t *testing.T) {
		testCtx := NewTestContext(1)
		chain := NewChain[*TestContext]()

		executed := false
		chain.AddNode(NewNode("single", func(ctx context.Context, c *TestContext) error {
			executed = true
			c.Log("single node executed")
			return nil
		}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build single node chain: %v", err)
		}

		if err := chain.Exec(context.Background(), testCtx); err != nil {
			t.Fatalf("Failed to execute single node chain: %v", err)
		}

		if !executed {
			t.Error("Single node should have executed")
		}

		results := testCtx.GetResults()
		if len(results) != 1 || results[0] != "single node executed" {
			t.Errorf("Expected single execution, got: %v", results)
		}
	})

	t.Run("chain with nil context", func(t *testing.T) {
		testCtx := NewTestContext(1)
		chain := NewChain[*TestContext]()

		chain.AddNode(NewNode("test", func(ctx context.Context, c *TestContext) error {
			c.Log("should not execute")
			return nil
		}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain: %v", err)
		}

		err := chain.Exec(nil, testCtx)
		// Chain doesn't validate nil context, so this should succeed
		if err != nil {
			t.Errorf("Expected no error with nil context, got: %v", err)
		}
	})

	t.Run("chain with mixed success and failure", func(t *testing.T) {
		testCtx := NewTestContext(10)
		chain := NewChain[*TestContext]()

		var executionOrder []string
		var orderMutex sync.Mutex

		addExecution := func(name string) {
			orderMutex.Lock()
			executionOrder = append(executionOrder, name)
			orderMutex.Unlock()
		}

		// Success -> Failure -> Should not execute
		chain.AddNode(NewNode("success1", func(ctx context.Context, c *TestContext) error {
			addExecution("success1")
			c.Log("success1 executed")
			return nil
		})).
			AddNode(NewNode("failure", func(ctx context.Context, c *TestContext) error {
				addExecution("failure")
				c.Log("failure executed")
				return fmt.Errorf("intentional failure")
			})).
			AddNode(NewNode("should_not_execute", func(ctx context.Context, c *TestContext) error {
				addExecution("should_not_execute")
				c.Log("should_not_execute executed")
				return nil
			}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build chain: %v", err)
		}

		err := chain.Exec(context.Background(), testCtx)
		if err == nil {
			t.Fatal("Expected chain to fail, but it succeeded")
		}

		if !strings.Contains(err.Error(), "intentional failure") {
			t.Errorf("Expected failure message, got: %v", err)
		}

		orderMutex.Lock()
		order := make([]string, len(executionOrder))
		copy(order, executionOrder)
		orderMutex.Unlock()

		expectedOrder := []string{"success1", "failure"}
		if len(order) != len(expectedOrder) {
			t.Fatalf("Expected %d executions, got %d: %v", len(expectedOrder), len(order), order)
		}

		for i, expected := range expectedOrder {
			if order[i] != expected {
				t.Errorf("Expected execution %d to be %s, got %s", i, expected, order[i])
			}
		}

		// Verify the node after failure didn't execute
		for _, exec := range order {
			if exec == "should_not_execute" {
				t.Error("Node after failure should not have executed")
			}
		}
	})

	t.Run("extremely long chain", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping long chain test in short mode")
		}

		const chainLength = 2000
		testCtx := NewTestContext(chainLength)
		chain := NewChain[*TestContext]()

		// Create very long chain
		for i := 0; i < chainLength; i++ {
			nodeName := fmt.Sprintf("long_chain_%d", i)
			chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
				c.Inc()
				return nil
			}))
		}

		start := time.Now()
		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build extremely long chain: %v", err)
		}
		buildTime := time.Since(start)

		start = time.Now()
		if err := chain.Exec(context.Background(), testCtx); err != nil {
			t.Fatalf("Failed to execute extremely long chain: %v", err)
		}
		execTime := time.Since(start)

		if buildTime > 5*time.Second {
			t.Errorf("Building extremely long chain took too long: %v", buildTime)
		}

		if execTime > 30*time.Second {
			t.Errorf("Executing extremely long chain took too long: %v", execTime)
		}

		if testCtx.Count() != chainLength {
			t.Errorf("Expected %d executions, got %d", chainLength, testCtx.Count())
		}
	})
}

// TestChainComplexScenarios tests complex real-world scenarios
func TestChainComplexScenarios(t *testing.T) {
	t.Run("pipeline with error recovery", func(t *testing.T) {
		testCtx := NewTestContext(20)
		chain := NewChain[*TestContext]()

		var recovered bool

		// Add error recovery middleware
		recoveryMW := func(node DependencyNode, next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				defer func() {
					if r := recover(); r != nil {
						recovered = true
						if tc, ok := req.(*TestContext); ok {
							tc.Log(fmt.Sprintf("recovered from panic in %s", node.Name()))
						}
					}
				}()
				return next(ctx, req)
			}
		}

		chain.AddGlobalMW(recoveryMW)

		// Normal -> Panic -> Recovery -> Normal
		chain.AddNode(NewNode("normal1", func(ctx context.Context, c *TestContext) error {
			c.Log("normal1 executed")
			return nil
		})).
			AddNode(NewNode("panic_node", func(ctx context.Context, c *TestContext) error {
				c.Log("panic_node executing")
				panic("test panic")
			})).
			AddNode(NewNode("normal2", func(ctx context.Context, c *TestContext) error {
				c.Log("normal2 executed")
				return nil
			}))

		if err := chain.Build(); err != nil {
			t.Fatalf("Failed to build recovery chain: %v", err)
		}

		// Should not panic the test
		chain.Exec(context.Background(), testCtx)

		if !recovered {
			t.Error("Expected panic to be recovered by middleware")
		}

		results := testCtx.GetResults()
		if !contains(results, "normal1 executed") {
			t.Error("First normal node should execute")
		}
		if !contains(results, "panic_node executing") {
			t.Error("Panic node should start executing")
		}
	})

	t.Run("dynamic chain modification simulation", func(t *testing.T) {
		// Simulate dynamic behavior by creating multiple chains with different structures
		scenarios := []struct {
			name      string
			nodeCount int
			hasGroup  bool
		}{
			{"small_chain", 3, false},
			{"medium_chain", 10, true},
			{"large_chain", 50, true},
		}

		for _, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				testCtx := NewTestContext(scenario.nodeCount * 2)
				chain := NewChain[*TestContext]()

				// Add nodes
				for i := 0; i < scenario.nodeCount; i++ {
					nodeName := fmt.Sprintf("%s_node_%d", scenario.name, i)
					chain.AddNode(NewNode(nodeName, func(ctx context.Context, c *TestContext) error {
						c.Inc()
						c.Log(fmt.Sprintf("%s executed", nodeName))
						return nil
					}))
				}

				// Optionally add a group
				if scenario.hasGroup {
					group := NewGroup[*TestContext](fmt.Sprintf("%s_group", scenario.name))
					for i := 0; i < 5; i++ {
						groupNodeName := fmt.Sprintf("%s_group_node_%d", scenario.name, i)
						group.AddNode(NewNode(groupNodeName, func(ctx context.Context, c *TestContext) error {
							c.Inc()
							c.Log(fmt.Sprintf("%s executed", groupNodeName))
							return nil
						}))
					}
					chain.AddNode(group.AsNode())
				}

				if err := chain.Build(); err != nil {
					t.Fatalf("Failed to build %s: %v", scenario.name, err)
				}

				if err := chain.Exec(context.Background(), testCtx); err != nil {
					t.Fatalf("Failed to execute %s: %v", scenario.name, err)
				}

				expectedCount := scenario.nodeCount
				if scenario.hasGroup {
					expectedCount += 5
				}

				if testCtx.Count() != expectedCount {
					t.Errorf("Expected %d executions for %s, got %d",
						expectedCount, scenario.name, testCtx.Count())
				}
			})
		}
	})
}

// Helper function to check if a slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
