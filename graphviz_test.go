package dagpher

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
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

	err := group.AsNode().Exec(ctx, 42)
	if err != nil {
		t.Fatalf("Failed to execute group: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Graph info: %s\n", info)
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

	err := group1.AsNode().Exec(ctx, "test")
	if err != nil {
		t.Fatalf("Failed to execute group1: %v", err)
	}

	err = group2.AsNode().Exec(ctx, "test")
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

	// 执行会有错误，但仍然会记录到 graphviz
	_ = group.AsNode().Exec(ctx, 42)

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Error graph info: %s\n", info)
	}
}

func TestGraphvizWithConcurrencyLimit(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("ConcurrencyLimitTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建测试节点 - 设计一个场景：同时有3个节点可以执行，但MaxGoNum=2
	// A (无依赖，50ms)
	// B (无依赖，100ms)
	// C (无依赖，80ms)
	// D (依赖A，60ms)
	// E (依赖B和C，40ms)
	//
	// 期望的执行序列：
	// 1. A和B同时开始 (2个goroutine)
	// 2. A完成后，C开始 (因为还有C在等待)
	// 3. B和C完成后，D和E可以开始

	nodeA := NewNode("A", func(ctx context.Context, c int) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	nodeB := NewNode("B", func(ctx context.Context, c int) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	nodeC := NewNode("C", func(ctx context.Context, c int) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	nodeD := NewNode("D", func(ctx context.Context, c int) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	}, "A")

	nodeE := NewNode("E", func(ctx context.Context, c int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "B", "C")

	// 创建 Group 并设置并发限制为2
	group := NewGroup[int]("concurrency_test")
	group.SetMaxGoNum(2) // 关键：限制并发度为2
	group.AddMiddleware(GraphvizMW())

	// 添加节点到组
	group.AddNode(nodeA)
	group.AddNode(nodeB)
	group.AddNode(nodeC)
	group.AddNode(nodeD)
	group.AddNode(nodeE)

	start := time.Now()
	err := group.AsNode().Exec(ctx, 42)
	totalTime := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute group: %v", err)
	}

	// 验证总执行时间
	// 理论上的执行序列：A(0-50) + B(0-100) 并行，然后 C(50-130) + D(50-110)，最后 E(130-170)
	// 总时间应该约为170ms
	expectedMin := 170 * time.Millisecond
	expectedMax := 190 * time.Millisecond

	if totalTime < expectedMin {
		t.Errorf("Total execution time %v is less than expected minimum %v", totalTime, expectedMin)
	}
	if totalTime > expectedMax {
		t.Errorf("Total execution time %v is greater than expected maximum %v", totalTime, expectedMax)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Concurrency limit test graph info: %s\n", info)
	}

	t.Logf("Total execution time: %v", totalTime)
}

func TestGroupExec(t *testing.T) {
	ctx := context.TODO()
	Convey("group", t, func() {
		var (
			graph  = NewGraph[*Tuple2]()
			exeCtx = &Tuple2{
				First:  1,
				Second: 1,
			}
		)

		g1 := NewGroup[*Tuple2]("group1")
		g1.SetMaxGoNum(10)
		A, B, C, D, E := NewCalcNodes(Param{SetDep: true, SetName: 1})
		g1.AddNode(A)
		g1.AddNode(B)
		g1.AddNode(C)
		g1.AddNode(D)
		g1.AddNode(E) // (31,153)
		// ((First + 3) * 5) + 11
		// ((Second + 3) * 5 * 7) + 13
		// 330

		g2 := NewGroup[*Tuple2]("group2", "group1")
		g2.SetMaxGoNum(1)
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 2})
		g2.AddNode(A)
		g2.AddNode(B)
		g2.AddNode(C)
		g2.AddNode(D)
		g2.AddNode(E) // (181, 5473)

		g3 := NewGroup[*Tuple2]("group3", "group2")
		g3.SetMaxGoNum(10)
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 3})
		g3.AddNode(A)
		g3.AddNode(B)
		g3.AddNode(C)
		g3.AddNode(D)
		g3.AddNode(E) // (931,191673)

		g4 := NewGroup[*Tuple2]("group4")
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 4})
		g4.AddNode(A)
		g4.AddNode(B)
		g4.AddNode(C)
		g4.AddNode(D)
		g4.AddNode(E) // (4681, 6711801)

		g3.AddNode(g4.AsNode())

		graph.AddNode(g1.AsNode())
		graph.AddNode(g2.AsNode())
		graph.AddNode(g3.AsNode())
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 5})
		graph.AddNode(A)

		ctx, graphviz := newGraphvizBuilder("GroupExec").Build(ctx)
		defer graphviz.Log(ctx)
		graph.AddGlobalMW(GraphvizMW())
		graph.AddGlobalMW(LoggerMW())

		now := time.Now()
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 4697) // 这里因为存在并发，不是4681
		So(exeCtx.Second, ShouldEqual, 6711801)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*1450)
		So(cost, ShouldBeLessThan, time.Millisecond*1459)
	})
}
