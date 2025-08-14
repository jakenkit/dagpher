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
		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 4})
		g4.AddNode(A)
		g4.AddNode(B)
		g4.AddNode(C)
		g4.AddNode(D)
		g4.AddNode(E) // (4681, 6711801)

		g3.AddNode(g4.AsNode())

		graph.AddNode(g1.AsNode())
		graph.AddNode(g2.AsNode())
		graph.AddNode(g3.AsNode())

		tmpA := NewNode("tmpA", func(ctx context.Context, c *Tuple2) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		}, "group1")
		graph.AddNode(tmpA)

		tmpB := NewNode("tmpB", func(ctx context.Context, c *Tuple2) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}, "group2")
		graph.AddNode(tmpB)

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

func TestGroupToGroupDependency(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("GroupToGroupTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建第一个组 - 数据准备组
	prepGroup := NewGroup[string]("prep")
	prepGroup.AddMiddleware(GraphvizMW())

	nodePrep1 := NewNode("PrepData", func(ctx context.Context, c string) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	nodePrep2 := NewNode("ValidateData", func(ctx context.Context, c string) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "PrepData")

	prepGroup.AddNode(nodePrep1)
	prepGroup.AddNode(nodePrep2)

	// 创建第二个组 - 处理组，依赖于prep组
	processGroup := NewGroup[string]("process", "prep")
	processGroup.AddMiddleware(GraphvizMW())

	nodeProcess1 := NewNode("ProcessA", func(ctx context.Context, c string) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	nodeProcess2 := NewNode("ProcessB", func(ctx context.Context, c string) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	}, "ProcessA")

	processGroup.AddNode(nodeProcess1)
	processGroup.AddNode(nodeProcess2)

	// 创建第三个组 - 输出组，依赖于process组
	outputGroup := NewGroup[string]("output", "process")
	outputGroup.AddMiddleware(GraphvizMW())

	nodeOutput := NewNode("GenerateReport", func(ctx context.Context, c string) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	})

	outputGroup.AddNode(nodeOutput)

	// 创建图并添加组
	mainGraph := NewGraph[string]()
	mainGraph.AddGlobalMW(GraphvizMW())
	mainGraph.AddNode(prepGroup.AsNode())
	mainGraph.AddNode(processGroup.AsNode())
	mainGraph.AddNode(outputGroup.AsNode())

	err := mainGraph.Exec(ctx, "test data")
	if err != nil {
		t.Fatalf("Failed to execute graph: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Group-to-Group dependency graph info: %s\n", info)
	}
}

func TestNodeToGroupDependency(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("NodeToGroupTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建服务启动组
	serviceGroup := NewGroup[int]("services")
	serviceGroup.AddMiddleware(GraphvizMW())

	dbNode := NewNode("StartDB", func(ctx context.Context, c int) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	cacheNode := NewNode("StartCache", func(ctx context.Context, c int) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	})

	apiNode := NewNode("StartAPI", func(ctx context.Context, c int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "StartDB", "StartCache")

	serviceGroup.AddNode(dbNode)
	serviceGroup.AddNode(cacheNode)
	serviceGroup.AddNode(apiNode)

	// 创建依赖于整个服务组的独立节点
	healthCheckNode := NewNode("HealthCheck", func(ctx context.Context, c int) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "services") // 依赖整个services组

	monitorNode := NewNode("StartMonitoring", func(ctx context.Context, c int) error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}, "services") // 依赖整个services组

	// 创建依赖于监控的节点
	alertNode := NewNode("SetupAlerts", func(ctx context.Context, c int) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, "StartMonitoring")

	// 创建图并添加节点
	mainGraph := NewGraph[int]()
	mainGraph.AddGlobalMW(GraphvizMW())
	mainGraph.AddNode(serviceGroup.AsNode())
	mainGraph.AddNode(healthCheckNode)
	mainGraph.AddNode(monitorNode)
	mainGraph.AddNode(alertNode)

	err := mainGraph.Exec(ctx, 42)
	if err != nil {
		t.Fatalf("Failed to execute graph: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Node-to-Group dependency graph info: %s\n", info)
	}
}

func TestComplexMixedDependencies(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("ComplexMixedTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建基础设施组
	infraGroup := NewGroup[string]("infrastructure")
	infraGroup.AddMiddleware(GraphvizMW())

	networkNode := NewNode("SetupNetwork", func(ctx context.Context, c string) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	storageNode := NewNode("SetupStorage", func(ctx context.Context, c string) error {
		time.Sleep(90 * time.Millisecond)
		return nil
	})

	infraGroup.AddNode(networkNode)
	infraGroup.AddNode(storageNode)

	// 创建应用组，依赖于基础设施组
	appGroup := NewGroup[string]("application", "infrastructure")
	appGroup.AddMiddleware(GraphvizMW())

	webServerNode := NewNode("StartWebServer", func(ctx context.Context, c string) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	appGroup.AddNode(webServerNode)

	// 创建独立的配置节点，依赖于基础设施组
	configNode := NewNode("LoadConfig", func(ctx context.Context, c string) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "infrastructure")

	// 创建部署节点，依赖于应用组和配置节点
	deployNode := NewNode("Deploy", func(ctx context.Context, c string) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "application", "LoadConfig")

	// 创建独立的测试节点，依赖于部署节点
	integrationTestNode := NewNode("IntegrationTest", func(ctx context.Context, c string) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	}, "Deploy")

	loadTestNode := NewNode("LoadTest", func(ctx context.Context, c string) error {
		time.Sleep(70 * time.Millisecond)
		return nil
	}, "Deploy")

	// 创建图并添加所有节点
	mainGraph := NewGraph[string]()
	mainGraph.AddGlobalMW(GraphvizMW())
	mainGraph.AddNode(infraGroup.AsNode())
	mainGraph.AddNode(appGroup.AsNode())
	mainGraph.AddNode(configNode)
	mainGraph.AddNode(deployNode)
	mainGraph.AddNode(integrationTestNode)
	mainGraph.AddNode(loadTestNode)

	err := mainGraph.Exec(ctx, "deployment")
	if err != nil {
		t.Fatalf("Failed to execute graph: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Complex mixed dependencies graph info: %s\n", info)
	}
}

func TestNestedGroupDependencies(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("NestedGroupTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建顶层组 - 数据层
	dataGroup := NewGroup[int]("data")
	dataGroup.AddMiddleware(GraphvizMW())

	dbInitNode := NewNode("InitDB", func(ctx context.Context, c int) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	})

	dataGroup.AddNode(dbInitNode)

	// 创建嵌套的数据访问子组
	dataAccessGroup := NewGroup[int]("access")
	dataAccessGroup.AddMiddleware(GraphvizMW())

	repoNode := NewNode("SetupRepo", func(ctx context.Context, c int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "InitDB")

	cacheLayerNode := NewNode("SetupCache", func(ctx context.Context, c int) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "SetupRepo")

	dataAccessGroup.AddNode(repoNode)
	dataAccessGroup.AddNode(cacheLayerNode)

	// 将子组添加到父组
	dataGroup.AddNode(dataAccessGroup.AsNode())

	// 创建业务逻辑组，依赖于整个数据组
	businessGroup := NewGroup[int]("business", "data")
	businessGroup.AddMiddleware(GraphvizMW())

	serviceNode := NewNode("BusinessService", func(ctx context.Context, c int) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	validatorNode := NewNode("Validator", func(ctx context.Context, c int) error {
		time.Sleep(35 * time.Millisecond)
		return nil
	}, "BusinessService")

	businessGroup.AddNode(serviceNode)
	businessGroup.AddNode(validatorNode)

	// 创建API层节点，依赖于业务组
	apiNode := NewNode("StartAPI", func(ctx context.Context, c int) error {
		time.Sleep(45 * time.Millisecond)
		return nil
	}, "business")

	// 创建嵌套的监控组
	monitoringGroup := NewGroup[int]("monitoring")
	monitoringGroup.AddMiddleware(GraphvizMW())

	// 监控子组中的健康检查组
	healthGroup := NewGroup[int]("health")
	healthGroup.AddMiddleware(GraphvizMW())

	healthCheckNode := NewNode("HealthCheck", func(ctx context.Context, c int) error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}, "StartAPI")

	healthGroup.AddNode(healthCheckNode)

	// 监控子组中的指标组
	metricsGroup := NewGroup[int]("metrics")
	metricsGroup.AddMiddleware(GraphvizMW())

	metricsCollectorNode := NewNode("MetricsCollector", func(ctx context.Context, c int) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, "StartAPI")

	metricsGroup.AddNode(metricsCollectorNode)

	// 将子组添加到监控组
	monitoringGroup.AddNode(healthGroup.AsNode())
	monitoringGroup.AddNode(metricsGroup.AsNode())

	// 创建告警节点，依赖于整个监控组
	alertNode := NewNode("AlertSystem", func(ctx context.Context, c int) error {
		time.Sleep(15 * time.Millisecond)
		return nil
	}, "monitoring")

	// 创建主图
	mainGraph := NewGraph[int]()
	mainGraph.AddGlobalMW(GraphvizMW())
	mainGraph.AddNode(dataGroup.AsNode())
	mainGraph.AddNode(businessGroup.AsNode())
	mainGraph.AddNode(apiNode)
	mainGraph.AddNode(monitoringGroup.AsNode())
	mainGraph.AddNode(alertNode)

	err := mainGraph.Exec(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to execute nested group graph: %v", err)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Nested group dependencies graph info: %s\n", info)
	}

	// 验证执行结果
	t.Log("Nested group dependency test completed successfully")
}

func TestSimpleGroupDependencyChain(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("SimpleGroupChain").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 创建第一个组 - 没有依赖
	groupA := NewGroup[string]("groupA")
	groupA.AddMiddleware(GraphvizMW())
	nodeA := NewNode("TaskA", func(ctx context.Context, c string) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	groupA.AddNode(nodeA)

	// 创建第二个组 - 依赖groupA
	groupB := NewGroup[string]("groupB", "groupA")
	groupB.AddMiddleware(GraphvizMW())
	nodeB := NewNode("TaskB", func(ctx context.Context, c string) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	})
	groupB.AddNode(nodeB)

	// 创建第三个组 - 依赖groupB
	groupC := NewGroup[string]("groupC", "groupB")
	groupC.AddMiddleware(GraphvizMW())
	nodeC := NewNode("TaskC", func(ctx context.Context, c string) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	})
	groupC.AddNode(nodeC)

	// 创建独立节点 - 依赖groupA
	independentNode := NewNode("IndependentTask", func(ctx context.Context, c string) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "groupA")

	// 创建最终节点 - 依赖groupC和独立节点
	finalNode := NewNode("FinalTask", func(ctx context.Context, c string) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, "groupC", "IndependentTask")

	// 创建主图
	mainGraph := NewGraph[string]()
	mainGraph.AddGlobalMW(GraphvizMW())
	mainGraph.AddNode(groupA.AsNode())
	mainGraph.AddNode(groupB.AsNode())
	mainGraph.AddNode(groupC.AsNode())
	mainGraph.AddNode(independentNode)
	mainGraph.AddNode(finalNode)

	start := time.Now()
	err := mainGraph.Exec(ctx, "test")
	totalTime := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute simple group chain: %v", err)
	}

	// 验证执行时间：应该是串行执行，大约 50+60+40+max(30,20) = 180ms
	expectedMin := 180 * time.Millisecond
	expectedMax := 220 * time.Millisecond

	if totalTime < expectedMin {
		t.Errorf("Total execution time %v is less than expected minimum %v", totalTime, expectedMin)
	}
	if totalTime > expectedMax {
		t.Errorf("Total execution time %v is greater than expected maximum %v", totalTime, expectedMax)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Simple group dependency chain graph info: %s\n", info)
	}

	t.Logf("Simple group chain test completed in %v", totalTime)
}

func TestWellDesignedComplexDependencies(t *testing.T) {
	ctx := context.Background()

	// 创建 graphviz 实例
	ctx, graph := newGraphvizBuilder("WellDesignedComplexTest").WithTimeDetail().Build(ctx)
	defer graph.Log(ctx)

	// 第一层：基础设施组 - 没有依赖
	infraGroup := NewGroup[int]("infrastructure")
	infraGroup.AddMiddleware(GraphvizMW())

	networkSetupNode := NewNode("NetworkSetup", func(ctx context.Context, c int) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	})

	storageSetupNode := NewNode("StorageSetup", func(ctx context.Context, c int) error {
		time.Sleep(70 * time.Millisecond)
		return nil
	}, "NetworkSetup")

	infraGroup.AddNode(networkSetupNode)
	infraGroup.AddNode(storageSetupNode)

	// 第二层：数据库组 - 依赖于基础设施组
	databaseGroup := NewGroup[int]("database", "infrastructure")
	databaseGroup.AddMiddleware(GraphvizMW())

	dbInstallNode := NewNode("DBInstall", func(ctx context.Context, c int) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	dbConfigNode := NewNode("DBConfig", func(ctx context.Context, c int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "DBInstall")

	databaseGroup.AddNode(dbInstallNode)
	databaseGroup.AddNode(dbConfigNode)

	// 第三层：应用服务组 - 依赖于数据库组
	appGroup := NewGroup[int]("application", "database")
	appGroup.AddMiddleware(GraphvizMW())

	appServerNode := NewNode("AppServer", func(ctx context.Context, c int) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	apiGatewayNode := NewNode("APIGateway", func(ctx context.Context, c int) error {
		time.Sleep(45 * time.Millisecond)
		return nil
	}, "AppServer")

	appGroup.AddNode(appServerNode)
	appGroup.AddNode(apiGatewayNode)

	// 独立节点：配置加载器 - 依赖于基础设施组
	configLoaderNode := NewNode("ConfigLoader", func(ctx context.Context, c int) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}, "infrastructure")

	// 独立节点：健康检查 - 依赖于应用组
	healthCheckNode := NewNode("HealthCheck", func(ctx context.Context, c int) error {
		time.Sleep(25 * time.Millisecond)
		return nil
	}, "application")

	// 第四层：监控组 - 依赖于应用组和健康检查节点
	monitorGroup := NewGroup[int]("monitoring")
	monitorGroup.AddMiddleware(GraphvizMW())

	metricsCollectorNode := NewNode("MetricsCollector", func(ctx context.Context, c int) error {
		time.Sleep(35 * time.Millisecond)
		return nil
	}, "HealthCheck")

	logAggregatorNode := NewNode("LogAggregator", func(ctx context.Context, c int) error {
		time.Sleep(40 * time.Millisecond)
		return nil
	}, "HealthCheck")

	monitorGroup.AddNode(metricsCollectorNode)
	monitorGroup.AddNode(logAggregatorNode)

	// 最终节点：部署完成通知 - 依赖于监控组和配置加载器
	deploymentCompleteNode := NewNode("DeploymentComplete", func(ctx context.Context, c int) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}, "monitoring", "ConfigLoader")

	// 创建主图并添加所有节点和组
	mainGraph := NewGraph[int]()
	mainGraph.AddGlobalMW(GraphvizMW())

	// 添加组
	mainGraph.AddNode(infraGroup.AsNode())
	mainGraph.AddNode(databaseGroup.AsNode())
	mainGraph.AddNode(appGroup.AsNode())
	mainGraph.AddNode(monitorGroup.AsNode())

	// 添加独立节点
	mainGraph.AddNode(configLoaderNode)
	mainGraph.AddNode(healthCheckNode)
	mainGraph.AddNode(deploymentCompleteNode)

	start := time.Now()
	err := mainGraph.Exec(ctx, 42)
	totalTime := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to execute well-designed complex graph: %v", err)
	}

	// 验证执行时间：应该是层次化执行
	// 估算：基础设施(130ms) -> 数据库(120ms) -> 应用(95ms) -> 配置和健康检查并行 -> 监控(75ms) -> 完成(20ms)
	// 大约 440ms
	expectedMin := 400 * time.Millisecond
	expectedMax := 500 * time.Millisecond

	if totalTime < expectedMin {
		t.Errorf("Total execution time %v is less than expected minimum %v", totalTime, expectedMin)
	}
	if totalTime > expectedMax {
		t.Errorf("Total execution time %v is greater than expected maximum %v", totalTime, expectedMax)
	}

	// 输出图信息
	info := graph.GetInfo()
	if info != "" {
		fmt.Printf("Well-designed complex dependencies graph info: %s\n", info)
	}

	t.Logf("Well-designed complex test completed in %v", totalTime)
}
