package dagpher

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestChainBasicExecution(t *testing.T) {
	ctx := context.Background()
	testCtx := &TestContext{}

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
	if len(testCtx.executionOrder) != len(expected) {
		t.Fatalf("Expected %d executions, got %d", len(expected), len(testCtx.executionOrder))
	}

	for i, expectedName := range expected {
		if testCtx.executionOrder[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, testCtx.executionOrder[i])
		}
	}
}

func TestChainWithGroups(t *testing.T) {
	ctx := context.Background()
	testCtx := &TestContext{}

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

	// Verify that before-group executed first and after-group executed last
	if len(testCtx.executionOrder) < 3 {
		t.Fatalf("Expected at least 3 executions, got %d", len(testCtx.executionOrder))
	}

	if testCtx.executionOrder[0] != "before-group" {
		t.Errorf("Expected first execution to be 'before-group', got %s", testCtx.executionOrder[0])
	}

	lastIndex := len(testCtx.executionOrder) - 1
	if testCtx.executionOrder[lastIndex] != "after-group" {
		t.Errorf("Expected last execution to be 'after-group', got %s", testCtx.executionOrder[lastIndex])
	}
}

func TestChainWithMiddleware(t *testing.T) {
	ctx := context.Background()
	testCtx := &TestContext{}

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
	if len(testCtx.executionOrder) != len(expected) {
		t.Fatalf("Expected %d executions, got %d: %v", len(expected), len(testCtx.executionOrder), testCtx.executionOrder)
	}

	for i, expectedName := range expected {
		if testCtx.executionOrder[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, testCtx.executionOrder[i])
		}
	}
}

func TestChainConcurrencyControl(t *testing.T) {
	ctx := context.Background()
	testCtx := &TestContext{}

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

	// Verify execution order is maintained
	if len(testCtx.executionOrder) != 6 {
		t.Fatalf("Expected 6 executions, got %d: %v", len(testCtx.executionOrder), testCtx.executionOrder)
	}
}

func TestChainErrorHandling(t *testing.T) {
	ctx := context.Background()
	testCtx := &TestContext{}

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
	if len(testCtx.executionOrder) != len(expected) {
		t.Fatalf("Expected %d executions, got %d: %v", len(expected), len(testCtx.executionOrder), testCtx.executionOrder)
	}

	for i, expectedName := range expected {
		if testCtx.executionOrder[i] != expectedName {
			t.Errorf("Expected execution %d to be %s, got %s", i, expectedName, testCtx.executionOrder[i])
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
