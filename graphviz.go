package dagpher

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awalterschulze/gographviz"
)

const (
	maxGraphCount           = 50
	maxGraphNodeCount       = 500
	maxOriginGraphUrlLength = 4096
)

type graphvizKey struct{}

// graphvizBuilder 用于构建graphviz实例
type graphvizBuilder struct {
	name        string
	minCostMs   int
	recordLimit int
	costDetail  bool
}

// newGraphvizBuilder 创建新的graphviz构建器
func newGraphvizBuilder(name string) *graphvizBuilder {
	return &graphvizBuilder{
		name:        name,
		minCostMs:   0,
		recordLimit: maxGraphNodeCount,
		costDetail:  false,
	}
}

// WithLimit 设置记录限制
func (b *graphvizBuilder) WithLimit(limit int) *graphvizBuilder {
	b.recordLimit = limit
	return b
}

// WithMinCost 设置最小耗时阈值
func (b *graphvizBuilder) WithMinCost(minCostMs int) *graphvizBuilder {
	b.minCostMs = minCostMs
	return b
}

// WithTimeDetail 启用时间详细信息
func (b *graphvizBuilder) WithTimeDetail() *graphvizBuilder {
	b.costDetail = true
	return b
}

// Build 构建graphviz实例并返回带有graphviz的context
func (b *graphvizBuilder) Build(ctx context.Context) (context.Context, *Graphviz) {
	g := newGraphviz(b.name, b.minCostMs, b.costDetail, true)
	return context.WithValue(ctx, graphvizKey{}, g), g
}

// graphNode 表示图中的一个节点
type graphNode struct {
	Node         DependencyNode
	GroupPath    string
	Dependencies []string
	Start        time.Time
	Finish       time.Time
	Error        error
	IsContainer  bool // true表示容器节点，false表示实际执行节点
}

// newGraphNode 创建新的图节点
func newGraphNode(node DependencyNode, groupPath string) *graphNode {
	return &graphNode{
		Node:         node,
		GroupPath:    groupPath,
		Dependencies: node.Dependencies(),
		IsContainer:  false,
	}
}

// record 记录节点的执行信息
func (n *graphNode) record(start, finish time.Time, err error) *graphNode {
	n.Start = start
	n.Finish = finish
	n.Error = err
	return n
}

// EdgeType 表示依赖边的类型
type EdgeType int

const (
	NodeToNode EdgeType = iota
	NodeToGroup
	GroupToNode
	GroupToGroup
)

// DependencyEdge 表示依赖关系边
type DependencyEdge struct {
	FromNode      string
	ToNode        string
	EdgeType      EdgeType
	IsLongest     bool
	GroupCross    bool
	FromGroupPath string
	ToGroupPath   string
}

// Graphviz 主要的graphviz管理结构
type Graphviz struct {
	name       string
	minCostMs  int
	costDetail bool
	start      time.Time
	valid      atomic.Bool
	mu         sync.Mutex
	nodes      []*graphNode
	nodeMap    map[DependencyNode]bool
	groups     map[string]bool
}

// newGraphviz 创建新的Graphviz实例
func newGraphviz(name string, minCostMs int, costDetail, valid bool) *Graphviz {
	g := &Graphviz{
		name:       name,
		minCostMs:  minCostMs,
		costDetail: costDetail,
		start:      time.Now(),
		valid:      atomic.Bool{},
		nodes:      []*graphNode{},
		nodeMap:    map[DependencyNode]bool{},
		groups:     map[string]bool{},
	}
	g.valid.Store(valid)
	return g
}

// record 记录节点执行信息
func (g *Graphviz) record(node DependencyNode, groupPath string, start, finish time.Time, err error) {
	if !g.valid.Load() {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.nodes) >= maxGraphNodeCount {
		g.valid.Store(false)
		return
	}

	g.nodes = append(g.nodes, newGraphNode(node, groupPath).record(start, finish, err))
	g.nodeMap[node] = true
}

// identifyContainerNodes 分析所有节点的group path，识别哪些节点是container节点
func (g *Graphviz) identifyContainerNodes() {
	// 收集所有的group path组件
	for _, node := range g.nodes {
		if node.GroupPath != "" {
			// 分割group path，提取每个层级的组名
			parts := strings.Split(node.GroupPath, HierarchyPathJoinChar)
			for _, part := range parts {
				if part != "" {
					g.groups[part] = true
				}
			}
		}
	}

	// 标记容器节点
	for _, node := range g.nodes {
		if g.groups[node.Node.Name()] {
			node.IsContainer = true
		}
	}
}

// Log 输出图信息到日志
func (g *Graphviz) Log(ctx context.Context) {
	info := g.GetInfo()
	if info != "" {
		fmt.Println(info)
	}
}

// GetInfo 生成graphviz可视化信息
func (g *Graphviz) GetInfo() string {
	if !g.valid.Load() {
		return ""
	}

	// 过滤耗时
	totalCostMs := time.Since(g.start).Milliseconds()
	if totalCostMs < int64(g.minCostMs) {
		return ""
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 数据预处理
	g.preprocessNodes()

	// 构建依赖图
	edges := g.buildDependencyGraph()

	// 基于group path分组
	groups := g.divideIntoGroupsByPath()

	// 计算最长路径
	g.calculateCriticalPaths(groups)

	// 渲染graphviz
	graph := g.renderGraphviz(groups, edges)

	// 生成结果
	return g.generateResult(graph, groups, totalCostMs)
}

// preprocessNodes 数据预处理
func (g *Graphviz) preprocessNodes() {
	// 识别容器节点
	g.identifyContainerNodes()
}

// buildDependencyGraph 构建统一的依赖图
func (g *Graphviz) buildDependencyGraph() []*DependencyEdge {
	var edges []*DependencyEdge

	// 创建节点名到节点的映射
	nodeByName := make(map[string]*graphNode)
	for _, node := range g.nodes {
		nodeByName[node.Node.Name()] = node
	}

	// 用于避免重复边的map
	edgeMap := make(map[string]bool)

	// 处理所有节点的依赖关系，但容器节点的依赖会被转换
	for _, node := range g.nodes {
		for _, depName := range node.Dependencies {
			// 生成边的唯一标识
			edgeKey := fmt.Sprintf("%s->%s", depName, node.Node.Name())
			if edgeMap[edgeKey] {
				continue // 跳过重复的边
			}

			if depNode, exists := nodeByName[depName]; exists {
				edge := &DependencyEdge{
					FromNode: depName,
					ToNode:   node.Node.Name(),
					EdgeType: g.classifyEdgeType(depNode, node),
					FromGroupPath: depNode.GroupPath,
					ToGroupPath:   node.GroupPath,
				}

				// 判断是否跨group依赖
				edge.GroupCross = g.isGroupCrossEdge(edge)

				// 调试信息：打印边的类型
				if edge.EdgeType == GroupToGroup {
					fmt.Printf("DEBUG Found GroupToGroup edge: %s (%v) -> %s (%v)\n",
						depName, depNode.IsContainer, node.Node.Name(), node.IsContainer)
				}

				edges = append(edges, edge)
				edgeMap[edgeKey] = true
			}
		}
	}

	return edges
}

// classifyEdgeType 分类边的类型
func (g *Graphviz) classifyEdgeType(fromNode, toNode *graphNode) EdgeType {
	switch {
	case !fromNode.IsContainer && !toNode.IsContainer:
		return NodeToNode
	case !fromNode.IsContainer && toNode.IsContainer:
		return NodeToGroup
	case fromNode.IsContainer && !toNode.IsContainer:
		return GroupToNode
	case fromNode.IsContainer && toNode.IsContainer:
		return GroupToGroup
	}
	return NodeToNode
}

// isGroupCrossEdge 判断是否是跨group的边
func (g *Graphviz) isGroupCrossEdge(edge *DependencyEdge) bool {
	return edge.FromGroupPath != edge.ToGroupPath
}

// graphGroup 表示一个group的信息
type graphGroup struct {
	GroupPath   string
	Nodes       []*graphNode
	NodeMap     map[*graphNode]bool
	First       *graphNode
	Last        *graphNode
	MinStart    time.Time
	MaxFinish   time.Time
	LongestPath []*graphNode
	LongestCost time.Duration
}

// divideIntoGroupsByPath 按group path分组节点
func (g *Graphviz) divideIntoGroupsByPath() []*graphGroup {
	// 按group path分组节点，只包含非容器节点用于可视化
	groupMap := make(map[string]*graphGroup)

	for _, node := range g.nodes {
		// 跳过容器节点，只处理实际执行的节点
		if node.IsContainer {
			continue
		}

		groupPath := node.GroupPath
		if _, exists := groupMap[groupPath]; !exists {
			groupMap[groupPath] = &graphGroup{
				GroupPath: groupPath,
				Nodes:     []*graphNode{},
				NodeMap:   map[*graphNode]bool{},
			}
		}

		group := groupMap[groupPath]
		group.Nodes = append(group.Nodes, node)
		group.NodeMap[node] = true
	}

	// 转换为slice并排序
	var groups []*graphGroup
	for _, group := range groupMap {
		if len(group.Nodes) > 0 {
			groups = append(groups, group)
		}
	}

	// 按group path排序，确保父组在子组之前
	g.sortGroupsByPath(groups)

	// 计算每组的统计信息
	g.calculateGroupStatistics(groups)

	return groups
}

// sortGroupsByPath 对groups按路径深度排序
func (g *Graphviz) sortGroupsByPath(groups []*graphGroup) {
	for i := 0; i < len(groups)-1; i++ {
		for j := i + 1; j < len(groups); j++ {
			depthI := strings.Count(groups[i].GroupPath, HierarchyPathJoinChar)
			depthJ := strings.Count(groups[j].GroupPath, HierarchyPathJoinChar)

			if depthI > depthJ {
				groups[i], groups[j] = groups[j], groups[i]
			} else if depthI == depthJ {
				if groups[i].GroupPath > groups[j].GroupPath {
					groups[i], groups[j] = groups[j], groups[i]
				}
			}
		}
	}
}

// calculateGroupStatistics 计算group的统计信息
func (g *Graphviz) calculateGroupStatistics(groups []*graphGroup) {
	for _, group := range groups {
		if len(group.Nodes) == 0 {
			continue
		}

		// 按开始时间排序节点
		g.sortNodesByStartTime(group.Nodes)

		group.First = group.Nodes[0]
		group.MinStart = group.First.Start
		group.MaxFinish = group.Nodes[0].Finish

		// 计算MinStart, MaxFinish和Last节点
		lastByTime := group.Nodes[0]
		for _, node := range group.Nodes {
			if node.Start.Before(group.MinStart) {
				group.MinStart = node.Start
			}
			if node.Finish.After(group.MaxFinish) {
				group.MaxFinish = node.Finish
			}
			if node.Finish.After(lastByTime.Finish) {
				lastByTime = node
			}
		}

		// 计算最长路径
		group.LongestPath, group.LongestCost = g.calculateLongestPath(group)

		// 确定Last节点
		if len(group.LongestPath) > 0 {
			lastInLongestPath := group.LongestPath[len(group.LongestPath)-1]
			if lastInLongestPath.Finish.Sub(lastByTime.Finish) > -2*time.Millisecond {
				group.Last = lastInLongestPath
			} else {
				group.Last = lastByTime
			}
		} else {
			group.Last = lastByTime
		}
	}
}

// sortNodesByStartTime 按开始时间排序节点
func (g *Graphviz) sortNodesByStartTime(nodes []*graphNode) {
	for i := 0; i < len(nodes)-1; i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[i].Start.After(nodes[j].Start) {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}

// calculateCriticalPaths 计算所有group的关键路径并标记longest edges
func (g *Graphviz) calculateCriticalPaths(groups []*graphGroup) {
	for _, group := range groups {
		group.LongestPath, group.LongestCost = g.calculateLongestPath(group)
	}
}

// calculateLongestPath 计算group内的最长路径
func (g *Graphviz) calculateLongestPath(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 检查是否为串行执行
	isSerialExecution := g.detectSerialExecution(group)

	if isSerialExecution {
		return g.calculateSerialExecutionPath(group)
	}

	// 并行执行模式：基于依赖关系计算关键路径
	return g.calculateParallelExecutionPath(group)
}

// detectSerialExecution 检测是否为串行执行
func (g *Graphviz) detectSerialExecution(group *graphGroup) bool {
	if len(group.Nodes) <= 1 {
		return false
	}

	// 按开始时间排序节点
	sortedNodes := make([]*graphNode, len(group.Nodes))
	copy(sortedNodes, group.Nodes)
	g.sortNodesByStartTime(sortedNodes)

	// 检查相邻节点的时间重叠
	overlapCount := 0
	totalPairs := len(sortedNodes) - 1

	for i := 0; i < len(sortedNodes)-1; i++ {
		current := sortedNodes[i]
		next := sortedNodes[i+1]

		if next.Start.Before(current.Finish) {
			overlapCount++
		}
	}

	// 如果重叠率小于30%，认为是串行执行
	overlapRatio := float64(overlapCount) / float64(totalPairs)
	return overlapRatio < 0.3
}

// calculateSerialExecutionPath 计算串行执行路径
func (g *Graphviz) calculateSerialExecutionPath(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 按时间顺序排序
	sortedNodes := make([]*graphNode, len(group.Nodes))
	copy(sortedNodes, group.Nodes)
	g.sortNodesByStartTime(sortedNodes)

	// 串行执行的最长路径就是所有节点的执行时间之和
	var totalDuration time.Duration
	for _, node := range sortedNodes {
		totalDuration += node.Finish.Sub(node.Start)
	}

	return sortedNodes, totalDuration
}

// calculateParallelExecutionPath 计算并行执行路径
func (g *Graphviz) calculateParallelExecutionPath(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 构建节点到索引的映射
	nodeToIndex := make(map[*graphNode]int)
	for i, node := range group.Nodes {
		nodeToIndex[node] = i
	}

	// 构建邻接列表（基于依赖关系）
	adj := make([][]int, len(group.Nodes))
	inDegree := make([]int, len(group.Nodes))

	for i, node := range group.Nodes {
		for _, depName := range node.Dependencies {
			// 在同group内查找依赖节点
			for j, depNode := range group.Nodes {
				if depNode.Node.Name() == depName {
					adj[j] = append(adj[j], i)
					inDegree[i]++
					break
				}
			}
		}
	}

	// 计算最长路径（基于实际执行时间）
	type pathInfo struct {
		path          []int
		totalDuration time.Duration
	}

	dp := make([]pathInfo, len(group.Nodes))
	for i := range dp {
		dp[i] = pathInfo{
			path:          []int{i},
			totalDuration: group.Nodes[i].Finish.Sub(group.Nodes[i].Start),
		}
	}

	// 拓扑排序 + 动态规划
	queue := []int{}
	currentInDegree := make([]int, len(inDegree))
	copy(currentInDegree, inDegree)

	for i, degree := range currentInDegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		for _, next := range adj[curr] {
			newDuration := dp[curr].totalDuration + group.Nodes[next].Finish.Sub(group.Nodes[next].Start)
			if newDuration > dp[next].totalDuration {
				dp[next].totalDuration = newDuration
				dp[next].path = append(append([]int{}, dp[curr].path...), next)
			}

			currentInDegree[next]--
			if currentInDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	// 找到最长路径
	maxDuration := time.Duration(0)
	maxPath := []int{}
	for _, info := range dp {
		if info.totalDuration > maxDuration {
			maxDuration = info.totalDuration
			maxPath = info.path
		}
	}

	// 转换为节点路径
	var longestPath []*graphNode
	for _, idx := range maxPath {
		longestPath = append(longestPath, group.Nodes[idx])
	}

	return longestPath, maxDuration
}

// renderGraphviz 渲染graphviz图
func (g *Graphviz) renderGraphviz(groups []*graphGroup, edges []*DependencyEdge) *gographviz.Graph {
	graph := gographviz.NewGraph()
	totalCostMs := time.Since(g.start).Milliseconds()
	graphAst, _ := gographviz.Parse([]byte(fmt.Sprintf(`digraph G{rankdir=LR; label="%v %vms";}`, g.name, totalCostMs)))
	_ = gographviz.Analyse(graphAst, graph)

	// 生成节点名映射
	nodeNames := g.generateNodeNames(groups)

	// 存储group path到graphviz cluster名称的映射
	groupPathToGraphName := make(map[string]string)

	// 绘制子图
	g.renderSubGraphs(graph, groups, groupPathToGraphName, nodeNames)

	// 绘制依赖边
	g.renderDependencyEdges(graph, edges, nodeNames, groups)

	return graph
}

// generateNodeNames 生成节点名映射，处理重名问题
func (g *Graphviz) generateNodeNames(groups []*graphGroup) map[DependencyNode]string {
	nodeNames := map[DependencyNode]string{}
	nameCount := map[string]int{}

	for _, group := range groups {
		for _, node := range group.Nodes {
			var name string
			if n, ok := nodeNames[node.Node]; ok {
				name = n
			} else if count := nameCount[node.Node.Name()]; count > 0 {
				name = fmt.Sprintf("%v_%v", node.Node.Name(), count)
				nodeNames[node.Node] = name
				nameCount[node.Node.Name()]++
			} else {
				name = node.Node.Name()
				nodeNames[node.Node] = name
				nameCount[node.Node.Name()]++
			}
		}
	}

	return nodeNames
}

// renderSubGraphs 渲染子图
func (g *Graphviz) renderSubGraphs(graph *gographviz.Graph, groups []*graphGroup, groupPathToGraphName map[string]string, nodeNames map[DependencyNode]string) {
	for idx, group := range groups {
		graphName := fmt.Sprintf("cluster_%v", idx)
		groupPathToGraphName[group.GroupPath] = graphName

		// 确定父图和标签
		parentGraphName := "G"
		groupLabel := group.GroupPath
		if lastSepIndex := strings.LastIndex(group.GroupPath, HierarchyPathJoinChar); lastSepIndex != -1 {
			parentGroupPath := group.GroupPath[:lastSepIndex]
			if name, ok := groupPathToGraphName[parentGroupPath]; ok {
				parentGraphName = name
			}
			groupLabel = group.GroupPath[lastSepIndex+1:]
		}

		if groupLabel != "" {
			_ = graph.AddSubGraph(parentGraphName, graphName, map[string]string{
				"label": fmt.Sprintf(`"%s\n%vms"`, groupLabel, group.MaxFinish.Sub(group.MinStart).Milliseconds()),
				"style": "solid",
			})
		}

		// 创建最长路径映射
		longestMap := map[DependencyNode]bool{}
		for _, node := range group.LongestPath {
			longestMap[node.Node] = true
		}

		// 绘制节点
		g.renderNodes(graph, graphName, group.Nodes, nodeNames, longestMap)
	}
}

// renderNodes 渲染节点
func (g *Graphviz) renderNodes(graph *gographviz.Graph, graphName string, nodes []*graphNode, nodeNames map[DependencyNode]string, longestMap map[DependencyNode]bool) {
	for _, node := range nodes {
		name := nodeNames[node.Node]
		var attr map[string]string

		if node.Error != nil {
			attr = map[string]string{
				"label":     fmt.Sprintf(`"%v\nError"`, name),
				"style":     "filled",
				"fillcolor": "red",
			}
		} else {
			labelBuilder := func(withLongest bool) string {
				str := fmt.Sprintf(`"%v\n`, name)
				if withLongest {
					str += "Longest"
				}
				str += fmt.Sprintf(`%vms`, node.Finish.Sub(node.Start).Milliseconds())
				if g.costDetail {
					str += fmt.Sprintf("\n[%v,%v]",
						node.Start.Sub(g.start).Milliseconds(),
						node.Finish.Sub(g.start).Milliseconds())
				}
				str += `"`
				return str
			}

			attr = map[string]string{
				"label": labelBuilder(false),
				"color": "green",
			}

			if longestMap[node.Node] {
				attr["color"] = "red"
				attr["style"] = "filled"
				attr["fillcolor"] = "lightcoral"
				if len(longestMap) == 1 {
					attr["label"] = labelBuilder(true)
				}
			}
		}

		_ = graph.AddNode(graphName, name, attr)
	}
}

// renderDependencyEdges 渲染依赖边
func (g *Graphviz) renderDependencyEdges(graph *gographviz.Graph, edges []*DependencyEdge, nodeNames map[DependencyNode]string, groups []*graphGroup) {
	// 创建节点名到节点的映射
	nodeByName := make(map[string]*graphNode)
	for _, node := range g.nodes {
		nodeByName[node.Node.Name()] = node
	}

	// 创建group名到group的映射，支持完整路径和组名两种方式
	groupNameToGroup := make(map[string]*graphGroup)
	for _, group := range groups {
		// 使用完整的GroupPath作为key
		groupNameToGroup[group.GroupPath] = group

		// 同时使用组名（路径的最后一部分）作为key，以便匹配节点依赖
		if lastSepIndex := strings.LastIndex(group.GroupPath, HierarchyPathJoinChar); lastSepIndex != -1 {
			groupName := group.GroupPath[lastSepIndex+1:]
			groupNameToGroup[groupName] = group
		}

		// 调试信息：打印group映射
		fmt.Printf("DEBUG Group mapping: '%s' -> %s (First: %v, Last: %v)\n",
			group.GroupPath, group.GroupPath,
			group.First != nil, group.Last != nil)
	}

	// 调试信息：打印所有可用的group名称
	fmt.Printf("DEBUG Available group names: ")
	for name := range groupNameToGroup {
		fmt.Printf("'%s' ", name)
	}
	fmt.Printf("\n")

	// 标记最长路径上的边
	g.markLongestEdges(edges, groups)

	// 用于跟踪已绘制的边，避免重复
	renderedEdges := make(map[string]bool)

	for _, edge := range edges {
		edgeKey := fmt.Sprintf("%s->%s", edge.FromNode, edge.ToNode)
		if !renderedEdges[edgeKey] {
			g.renderSingleEdge(graph, edge, nodeNames, nodeByName, groupNameToGroup)
			renderedEdges[edgeKey] = true
		}
	}
}

// markLongestEdges 标记最长路径上的边
func (g *Graphviz) markLongestEdges(edges []*DependencyEdge, groups []*graphGroup) {
	for _, group := range groups {
		if len(group.LongestPath) <= 1 {
			continue
		}

		// 标记group内最长路径的边
		for i := 0; i < len(group.LongestPath)-1; i++ {
			fromNode := group.LongestPath[i].Node.Name()
			toNode := group.LongestPath[i+1].Node.Name()

			for _, edge := range edges {
				if edge.FromNode == fromNode && edge.ToNode == toNode {
					edge.IsLongest = true
					break
				}
			}
		}
	}
}

// renderSingleEdge 渲染单条边
func (g *Graphviz) renderSingleEdge(graph *gographviz.Graph, edge *DependencyEdge, nodeNames map[DependencyNode]string, nodeByName map[string]*graphNode, groupNameToGroup map[string]*graphGroup) {
	attr := map[string]string{}

	fromNode := nodeByName[edge.FromNode]
	toNode := nodeByName[edge.ToNode]

	// 根据边类型设置样式
	switch edge.EdgeType {
	case NodeToNode:
		// 普通节点到节点的依赖 - 只有当两个节点都不是容器节点且在nodeNames中存在时才绘制
		if fromNode != nil && toNode != nil && !fromNode.IsContainer && !toNode.IsContainer {
			if fromNodeName, ok := nodeNames[fromNode.Node]; ok {
				if toNodeName, ok := nodeNames[toNode.Node]; ok {
					if edge.IsLongest {
						attr["color"] = "red"
						attr["style"] = "bold"
					}
					_ = graph.AddEdge(fromNodeName, toNodeName, true, attr)
				}
			}
		}

	case NodeToGroup:
		// 节点到group：连接到group的第一个节点
		if depGroup, exists := groupNameToGroup[edge.ToNode]; exists && depGroup.First != nil {
			attr["style"] = "dashed"
			attr["color"] = "blue"
			attr["label"] = fmt.Sprintf(`"%s"`, edge.ToNode)
			// 只有源节点不是容器节点且在nodeNames中存在时才绘制边
			if fromNode != nil && !fromNode.IsContainer {
				if fromNodeName, ok := nodeNames[fromNode.Node]; ok {
					if firstNodeName, ok := nodeNames[depGroup.First.Node]; ok {
						_ = graph.AddEdge(fromNodeName, firstNodeName, true, attr)
					}
				}
			}
		}

	case GroupToNode:
		// group到节点：从group的最后一个节点连接
		if depGroup, exists := groupNameToGroup[edge.FromNode]; exists && depGroup.Last != nil {
			attr["style"] = "dashed"
			attr["color"] = "purple"
			attr["label"] = fmt.Sprintf(`"%s->%s"`, edge.FromNode, edge.ToNode)
			// 只有目标节点不是容器节点且在nodeNames中存在时才绘制边
			if toNode != nil && !toNode.IsContainer {
				if toNodeName, ok := nodeNames[toNode.Node]; ok {
					if lastNodeName, ok := nodeNames[depGroup.Last.Node]; ok {
						_ = graph.AddEdge(lastNodeName, toNodeName, true, attr)
					}
				}
			}
		}

	case GroupToGroup:
		// group到group：从源group的最后一个节点到目标group的第一个节点
		fromGroup, fromExists := groupNameToGroup[edge.FromNode]
		toGroup, toExists := groupNameToGroup[edge.ToNode]

		// 调试信息
		fmt.Printf("DEBUG GroupToGroup: %s -> %s, fromExists: %v, toExists: %v\n",
			edge.FromNode, edge.ToNode, fromExists, toExists)
		if fromExists {
			fmt.Printf("DEBUG fromGroup: %s, Last: %v\n", fromGroup.GroupPath, fromGroup.Last != nil)
		}
		if toExists {
			fmt.Printf("DEBUG toGroup: %s, First: %v\n", toGroup.GroupPath, toGroup.First != nil)
		}

		if fromExists && toExists && fromGroup.Last != nil && toGroup.First != nil {
			attr["style"] = "dashed"
			attr["color"] = "orange"
			attr["label"] = fmt.Sprintf(`"%s->%s"`, edge.FromNode, edge.ToNode)
			// 确保两个边界节点都在nodeNames中存在
			if lastNodeName, ok := nodeNames[fromGroup.Last.Node]; ok {
				if firstNodeName, ok := nodeNames[toGroup.First.Node]; ok {
					fmt.Printf("DEBUG GroupToGroup edge added: %s -> %s\n", lastNodeName, firstNodeName)
					_ = graph.AddEdge(lastNodeName, firstNodeName, true, attr)
				} else {
					fmt.Printf("DEBUG GroupToGroup failed: firstNodeName not found for %s\n", toGroup.First.Node.Name())
				}
			} else {
				fmt.Printf("DEBUG GroupToGroup failed: lastNodeName not found for %s\n", fromGroup.Last.Node.Name())
			}
		}
	}
}

// generateResult 生成最终结果字符串
func (g *Graphviz) generateResult(graph *gographviz.Graph, groups []*graphGroup, totalCostMs int64) string {
	// 生成路径描述
	var pathParts []string
	for _, group := range groups {
		if len(group.LongestPath) > 0 {
			nodeNames := g.generateNodeNames([]*graphGroup{group})
			var nodeParts []string
			for _, node := range group.LongestPath {
				nodeParts = append(nodeParts, nodeNames[node.Node])
			}
			nodePath := strings.Join(nodeParts, " -> ")
			pathParts = append(pathParts, fmt.Sprintf("[%v(%vms)]", nodePath, group.LongestCost.Milliseconds()))
		}
	}
	path := strings.Join(pathParts, "")

	// 生成图URL
	graphUrl := g.compressGraphUrl(fmt.Sprintf("https://dreampuf.github.io/GraphvizOnline/?presentation#%v",
		url.PathEscape(graph.String())))

	return fmt.Sprintf("total cost: %vms, longest path: %v, graph: %v", totalCostMs, path, graphUrl)
}

// compressGraphUrl 压缩图URL
func (g *Graphviz) compressGraphUrl(originUrl string) string {
	if len(originUrl) <= maxOriginGraphUrlLength {
		return originUrl
	}

	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, _ = writer.Write([]byte(originUrl))
	_ = writer.Close()

	compressed := base64.StdEncoding.EncodeToString(buf.Bytes())
	return fmt.Sprintf("compressed: %s", compressed)
}

// GraphvizMW 返回graphviz中间件
func GraphvizMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			// 从context中获取graphviz实例
			graphviz, ok := ctx.Value(graphvizKey{}).(*Graphviz)
			if !ok || graphviz == nil {
				// 如果没有graphviz实例，直接执行下一个中间件
				return next(ctx, req)
			}
			// 获取当前的group path
			groupPath := GetCurrentGroupPath(ctx)

			start := time.Now()
			resp, err := next(ctx, req)
			finish := time.Now()

			// 记录节点执行信息，包含group path
			graphviz.record(node, groupPath, start, finish, err)

			return resp, err
		}
	}
}
