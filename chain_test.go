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
	chain := NewChain[*TestContext]("test-chain")

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
	chain := NewChain[*TestContext]("mixed-chain")
	chain.AddNode(NewNode("before-group", func(ctx context.Context, c *TestContext) error {
		c.AddExecution("before-group")
		return nil
	})).
		AddNode(group).
		AddNode(NewNode("after-group", func(ctx context.Context, c *TestContext) error {
			c.AddExecution("after-group")
			return nil
		}))

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
	chain := NewChain[*TestContext]("mw-chain")
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
	chain := NewChain[*TestContext]("serial-chain")
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
	chain := NewChain[*TestContext]("error-chain")
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
		chain := NewChain[*Tuple2]("serial-pipeline").SetMaxGoNum(1)

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
		chain := NewChain[*Tuple2]("parallel-pipeline")

		// Group B and C to run in parallel
		groupBC := NewGroup[*Tuple2]("group-bc")
		groupBC.AddNode(B)
		groupBC.AddNode(C)

		// Build the chain A -> Group(B,C) -> E -> D
		// Note: D depends on B, E depends on B and C.
		// To make the chain valid, we need to ensure dependencies are met.
		// A -> groupBC(B,C) -> E -> D is a valid sequence.
		chain.AddNode(A)
		chain.AddNode(groupBC)
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
