package dagpher

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGraphvizMiddleware(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("TestGraphvizMW").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建测试节点
	nodeA := NewNode("A", func(ctx context.Context, c int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	nodeB := NewNode("B", func(ctx context.Context, c int) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, "A")

	nodeC := NewNode("C", func(ctx context.Context, c int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}, "A")

	nodeD := NewNode("D", func(ctx context.Context, c int) error {
		time.Sleep(300 * time.Millisecond)
		return nil
	}, "B")

	nodeE := NewNode("E", func(ctx context.Context, c int) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "B", "C")

	// 创建 Group 并添加 graphviz 中间件
	group := NewGroup[int]("test_group")
	group.AddMiddleware(GraphvizMW())

	// 添加节点到组
	group.AddNode(nodeA)
	group.AddNode(nodeB)
	group.AddNode(nodeC)
	group.AddNode(nodeD)
	group.AddNode(nodeE)

	// 构建并执行
	err := group.Build()
	if err != nil {
		t.Fatalf("Failed to build group: %v", err)
	}

	err = group.Exec(ctx, 42)
	if err != nil {
		t.Fatalf("Failed to execute group: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Graph info: %s\n", info)
	}
}

func TestGraphvizWithNestedGroups(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("NestedGroupTest").WithMinCost(50).Build(ctx)
	defer graph.Log(ctx)

	// 创建内层组
	innerGroup := NewGroup[string]("inner_group")
	innerGroup.AddMiddleware(GraphvizMW())

	nodeA := NewNode("NodeA", func(ctx context.Context, c string) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	nodeB := NewNode("NodeB", func(ctx context.Context, c string) error {
		time.Sleep(150 * time.Millisecond)
		return nil
	}, "NodeA")

	innerGroup.AddNode(nodeA)
	innerGroup.AddNode(nodeB)

	// 创建外层组
	outerGroup := NewGroup[string]("outer_group")
	outerGroup.AddMiddleware(GraphvizMW())

	nodeC := NewNode("NodeC", func(ctx context.Context, c string) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	nodeD := NewNode("NodeD", func(ctx context.Context, c string) error {
		time.Sleep(120 * time.Millisecond)
		return nil
	}, "NodeC")

	// 将内层组作为节点添加到外层组
	outerGroup.AddNode(innerGroup)
	outerGroup.AddNode(nodeC)
	outerGroup.AddNode(nodeD)

	// 构建并执行组
	err := outerGroup.Build()
	if err != nil {
		t.Fatalf("Failed to build outer group: %v", err)
	}

	err = outerGroup.Exec(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to execute outer group: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Nested groups graph info: %s\n", info)
	}
}

func TestGraphvizWithMultipleGroups(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("MultiGroupTest").WithMinCost(50).Build(ctx)
	defer graph.Log(ctx)

	// 第一个组
	group1 := NewGroup[string]("group1")
	group1.AddMiddleware(GraphvizMW())

	nodeA := NewNode("GroupA_NodeA", func(ctx context.Context, c string) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	nodeB := NewNode("GroupA_NodeB", func(ctx context.Context, c string) error {
		time.Sleep(150 * time.Millisecond)
		return nil
	}, "GroupA_NodeA")

	group1.AddNode(nodeA)
	group1.AddNode(nodeB)

	// 第二个组
	group2 := NewGroup[string]("group2", "group1")
	group2.AddMiddleware(GraphvizMW())

	nodeC := NewNode("GroupB_NodeC", func(ctx context.Context, c string) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	nodeD := NewNode("GroupB_NodeD", func(ctx context.Context, c string) error {
		time.Sleep(120 * time.Millisecond)
		return nil
	}, "GroupB_NodeC")

	group2.AddNode(nodeC)
	group2.AddNode(nodeD)

	// 构建并执行组
	err := group1.Build()
	if err != nil {
		t.Fatalf("Failed to build group1: %v", err)
	}

	err = group2.Build()
	if err != nil {
		t.Fatalf("Failed to build group2: %v", err)
	}

	err = group1.Exec(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to execute group1: %v", err)
	}

	err = group2.Exec(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to execute group2: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Multi-group graph info: %s\n", info)
	}
}

func TestGraphvizWithErrors(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("ErrorTest").Build(ctx)
	defer graph.Log(ctx)

	// 创建会出错的节点
	errorNode := NewNode("ErrorNode", func(ctx context.Context, c int) error {
		time.Sleep(50 * time.Millisecond)
		return fmt.Errorf("test error")
	})

	successNode := NewNode("SuccessNode", func(ctx context.Context, c int) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "ErrorNode")

	group := NewGroup[int]("error_group")
	group.AddMiddleware(GraphvizMW())

	group.AddNode(errorNode)
	group.AddNode(successNode)

	err := group.Build()
	if err != nil {
		t.Fatalf("Failed to build group: %v", err)
	}

	// 执行会有错误，但仍然会记录到 graphviz
	_ = group.Exec(ctx, 42)

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Error graph info: %s\n", info)
	}
}

func TestGraphvizWithSingleNode(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("SingleNodeTest").Build(ctx)
	defer graph.Log(ctx)

	// 创建单个节点（这是你提到的会被单独框出来的情况）
	singleNode := NewNode("SingleNode", func(ctx context.Context, c int) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	group := NewGroup[int]("single_group")
	group.AddMiddleware(GraphvizMW())
	group.AddNode(singleNode)

	err := group.Build()
	if err != nil {
		t.Fatalf("Failed to build group: %v", err)
	}

	err = group.Exec(ctx, 42)
	if err != nil {
		t.Fatalf("Failed to execute group: %v", err)
	}

	// 输出图信息 - 现在单个节点不会被单独框出来，因为它属于 single_group
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Single node graph info: %s\n", info)
	}
}
