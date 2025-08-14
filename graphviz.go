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
	maxGraphCount           = 1024 // graph 最大数量, 避免全局变量过多
	maxGraphNodeCount       = 256  // graph 最大节点数量
	maxOriginGraphUrlLength = 4096 // 原始 graph url 最大长度, 超过压缩
)

type graphvizKey struct{}

type graphvizBuilder struct {
	name        string
	minCostMs   int
	recordLimit float64
	costDetail  bool
}

// newGraphvizBuilder
// name graph 名称, context 内唯一, 如果重复, 不记录
func newGraphvizBuilder(name string) *graphvizBuilder {
	return &graphvizBuilder{
		name:        name,
		minCostMs:   0,
		recordLimit: -1,
		costDetail:  false,
	}
}

// WithLimit 限流 QPS, 默认不限制. 为 0 则不启用中间件, <0 不限制
// 入口处限流, 先于 WithMinCost
func (b *graphvizBuilder) WithLimit(limit float64) *graphvizBuilder {
	b.recordLimit = limit
	return b
}

// WithMinCost 打印日志的最小耗时, 默认不限制
func (b *graphvizBuilder) WithMinCost(minCostMs int) *graphvizBuilder {
	b.minCostMs = minCostMs
	return b
}

// WithTimeDetail
// 开启耗时详情, 绘制每个节点的起始时间和终止时间, 会导致日志量增加, 默认关闭
func (b *graphvizBuilder) WithTimeDetail() *graphvizBuilder {
	b.costDetail = true
	return b
}

// Build 返回 ctx 和 Graphviz, 使用返回的 ctx 执行 Node
func (b *graphvizBuilder) Build(ctx context.Context) (context.Context, *Graphviz) {
	// 写入 ctx
	graph := newGraphviz(b.name, b.minCostMs, b.costDetail, true)
	return context.WithValue(ctx, graphvizKey{}, graph), graph
}

type graphNode struct {
	Node      DependencyNode
	GroupPath string // 添加 group path 信息

	Start  time.Time
	Finish time.Time
	Error  error
}

func newGraphNode(node DependencyNode, groupPath string) *graphNode {
	return &graphNode{
		Node:      node,
		GroupPath: groupPath,
	}
}

func (n *graphNode) record(start, finish time.Time, err error) *graphNode {
	n.Start = start
	n.Finish = finish
	n.Error = err
	return n
}

type Graphviz struct {
	name       string
	minCostMs  int
	costDetail bool
	start      time.Time
	valid      atomic.Bool // 是否有效
	mu         sync.Mutex
	nodes      []*graphNode
	nodeMap    map[DependencyNode]bool
}

func newGraphviz(name string, minCostMs int, costDetail, valid bool) *Graphviz {
	g := &Graphviz{
		name:       name,
		minCostMs:  minCostMs,
		costDetail: costDetail,
		start:      time.Now(),
		valid:      atomic.Bool{},
		nodes:      []*graphNode{},
		nodeMap:    map[DependencyNode]bool{},
	}
	g.valid.Store(valid)
	return g
}

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

	g.nodes = append(g.nodes, newGraphNode(node, groupPath).
		record(start, finish, err))
	g.nodeMap[node] = true
}

// Log 日志信息, 可能返回空. 返回空时不使用
func (g *Graphviz) Log(ctx context.Context) {
	info := g.GetInfo()
	if info != "" {
		// 这里可以根据需要使用不同的日志库
		fmt.Printf("[Graphviz] %s\n", info)
	}
}

// GetInfo 获取图信息
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

	// 节点名
	var (
		nodeNames = map[DependencyNode]string{}
		nameCount = map[string]int{}
	)
	for _, node := range g.nodes {
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

	// 基于实际的 group path 进行分组
	groups := g.divideIntoGroupsByPath()

	// 绘制
	graph := gographviz.NewGraph()
	graphAst, _ := gographviz.Parse([]byte(fmt.Sprintf(`digraph G{rankdir=LR; label="%v %vms";}`,
		g.name, totalCostMs)))
	_ = gographviz.Analyse(graphAst, graph)

	// 存储 group path 到 graphviz cluster 名称的映射
	groupPathToGraphName := make(map[string]string)

	// 绘制子图
	for idx, group := range groups {
		graphName := fmt.Sprintf("cluster_%v", idx)
		groupPathToGraphName[group.GroupPath] = graphName

		// 确定父图和标签
		parentGraphName := "G" // 默认为根图
		groupLabel := group.GroupPath
		if lastSepIndex := strings.LastIndex(group.GroupPath, HierarchyPathJoinChar); lastSepIndex != -1 {
			parentGroupPath := group.GroupPath[:lastSepIndex]
			if name, ok := groupPathToGraphName[parentGroupPath]; ok {
				parentGraphName = name
			}
			// 更新标签为子组名
			groupLabel = group.GroupPath[lastSepIndex+1:]
		}

		if groupLabel != "" {
			_ = graph.AddSubGraph(parentGraphName, graphName, map[string]string{
				"label": fmt.Sprintf(`"%s\n%vms"`, groupLabel, group.MaxFinish.Sub(group.MinStart).Milliseconds()),
				"style": "solid",
			})
		}

		longestMap := map[DependencyNode]bool{}
		for _, node := range group.LongestPath {
			longestMap[node.Node] = true
		}

		// 点
		for _, node := range group.Nodes {
			var (
				name = nodeNames[node.Node]
				attr map[string]string
			)
			if node.Error != nil {
				attr = map[string]string{
					"label":     fmt.Sprintf(`"%v\nError"`, name),
					"style":     "filled",
					"fillcolor": "red",
				}
			} else {
				buildLabel := func(withLongest bool) string {
					str := fmt.Sprintf(`"%v\n`, name)
					if withLongest {
						str += "Longest"
					}
					str += fmt.Sprintf(`%vms`, node.Finish.Sub(node.Start).Milliseconds())
					if g.costDetail {
						str += fmt.Sprintf("\n[%v,%v]", node.Start.Sub(g.start).Milliseconds(),
							node.Finish.Sub(g.start).Milliseconds())
					}
					str += `"`
					return str
				}
				attr = map[string]string{
					"label": buildLabel(false),
					"color": "green",
				}
				if longestMap[node.Node] {
					attr["color"] = "red"
					attr["style"] = "filled"
					attr["fillcolor"] = "lightcoral"
					if len(group.LongestPath) == 1 {
						attr["label"] = buildLabel(true)
					}
				}
			}
			_ = graph.AddNode(graphName, name, attr)
		}

		// 边
		markLongestLabel := false
		for _, node := range group.Nodes {
			for _, depName := range node.Node.Dependencies() {
				// 找到对应的依赖节点
				var depNode DependencyNode
				for _, n := range g.nodes {
					if n.Node.Name() == depName {
						depNode = n.Node
						break
					}
				}
				if depNode == nil || !g.nodeMap[depNode] {
					continue
				}
				attr := map[string]string{}
				if longestMap[node.Node] && longestMap[depNode] {
					attr["color"] = "red"
					if group.LongestPath[0].Node == depNode && !markLongestLabel {
						markLongestLabel = true
						attr["label"] = fmt.Sprintf("Longest%vms", group.LongestCost.Milliseconds())
					}
				}
				_ = graph.AddEdge(nodeNames[depNode], nodeNames[node.Node], true, attr)
			}
		}

		// 上一个分组的最后一个节点, 指向当前分组第一个节点
		if idx > 0 {
			preNode := groups[idx-1].Last.Node
			nextNode := group.First.Node
			if len(group.LongestPath) > 0 && group.LongestPath[0].Start.Sub(group.First.Start) < time.Millisecond*2 {
				nextNode = group.LongestPath[0].Node
			}
			_ = graph.AddEdge(nodeNames[preNode], nodeNames[nextNode], true, map[string]string{
				"style": "dashed",
			})
		}
	}

	var (
		path = strings.Join(Map(groups, func(group *graphGroup) string {
			path := strings.Join(Map(group.LongestPath, func(node *graphNode) string {
				return nodeNames[node.Node]
			}), "->")
			return fmt.Sprintf("[%v(%vms)]", path, group.LongestCost.Milliseconds())
		}), "")
		graphUrl = g.compressGraphUrl(fmt.Sprintf("https://dreampuf.github.io/GraphvizOnline/?presentation#%v",
			url.PathEscape(graph.String())))
	)
	return fmt.Sprintf("total cost: %vms, longest path: %v, graph: %v", totalCostMs, path, graphUrl)
}

// 分组
type graphGroup struct {
	GroupPath   string // 添加 group path 标识
	Nodes       []*graphNode
	NodeMap     map[*graphNode]bool
	First       *graphNode
	Last        *graphNode
	MinStart    time.Time
	MaxFinish   time.Time
	LongestPath []*graphNode
	LongestCost time.Duration
}

// divideIntoGroupsByPath 基于实际的 group path 进行分组
func (g *Graphviz) divideIntoGroupsByPath() []*graphGroup {
	// 按 group path 分组节点
	groupMap := make(map[string]*graphGroup)

	for _, node := range g.nodes {
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

	// 将 map 转换为 slice 并排序
	var groups []*graphGroup
	for _, group := range groupMap {
		if len(group.Nodes) > 0 {
			groups = append(groups, group)
		}
	}

	// 按 group path 排序，确保父组在子组之前
	for i := 0; i < len(groups)-1; i++ {
		for j := i + 1; j < len(groups); j++ {
			// 首先按路径深度排序（路径中 / 的数量）
			depthI := strings.Count(groups[i].GroupPath, HierarchyPathJoinChar)
			depthJ := strings.Count(groups[j].GroupPath, HierarchyPathJoinChar)

			if depthI > depthJ {
				groups[i], groups[j] = groups[j], groups[i]
			} else if depthI == depthJ {
				// 同一深度按字母顺序排序
				if groups[i].GroupPath > groups[j].GroupPath {
					groups[i], groups[j] = groups[j], groups[i]
				}
			}
		}
	}

	// 计算每组的统计信息
	for _, group := range groups {
		if len(group.Nodes) == 0 {
			continue
		}

		// 按时间排序节点
		for i := 0; i < len(group.Nodes)-1; i++ {
			for j := i + 1; j < len(group.Nodes); j++ {
				if group.Nodes[i].Start.After(group.Nodes[j].Start) {
					group.Nodes[i], group.Nodes[j] = group.Nodes[j], group.Nodes[i]
				}
			}
		}

		group.First = group.Nodes[0]
		group.Last = group.Nodes[len(group.Nodes)-1]
		group.MinStart = group.First.Start
		group.MaxFinish = group.Last.Finish

		for _, node := range group.Nodes {
			if node.Start.Before(group.MinStart) {
				group.MinStart = node.Start
			}
			if node.Finish.After(group.MaxFinish) {
				group.MaxFinish = node.Finish
			}
		}

		// 计算最长路径
		group.LongestPath, group.LongestCost = g.calculateLongestPath(group)
	}

	return groups
}

func (g *Graphviz) calculateLongestPath(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 基于实际执行时间计算关键路径
	return g.calculateCriticalPathFromActualTimes(group)
}

// calculateCriticalPathFromActualTimes 基于实际执行时间计算关键路径
func (g *Graphviz) calculateCriticalPathFromActualTimes(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 首先检查是否为串行执行模式
	// 如果节点按时间顺序基本没有重叠，则认为是串行执行
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
	for i := 0; i < len(sortedNodes)-1; i++ {
		for j := i + 1; j < len(sortedNodes); j++ {
			if sortedNodes[i].Start.After(sortedNodes[j].Start) {
				sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
			}
		}
	}

	// 检查相邻节点的时间重叠
	overlapCount := 0
	totalPairs := len(sortedNodes) - 1

	for i := 0; i < len(sortedNodes)-1; i++ {
		current := sortedNodes[i]
		next := sortedNodes[i+1]

		// 如果下一个节点在当前节点结束之前开始，则有重叠
		if next.Start.Before(current.Finish) {
			overlapCount++
		}
	}

	// 如果重叠率小于30%，认为是串行执行
	overlapRatio := float64(overlapCount) / float64(totalPairs)
	return overlapRatio < 0.3
}

// calculateSerialExecutionPath 计算串行执行的路径
func (g *Graphviz) calculateSerialExecutionPath(group *graphGroup) ([]*graphNode, time.Duration) {
	if len(group.Nodes) == 0 {
		return nil, 0
	}

	// 按实际开始时间排序所有节点
	sortedNodes := make([]*graphNode, len(group.Nodes))
	copy(sortedNodes, group.Nodes)
	for i := 0; i < len(sortedNodes)-1; i++ {
		for j := i + 1; j < len(sortedNodes); j++ {
			if sortedNodes[i].Start.After(sortedNodes[j].Start) {
				sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
			}
		}
	}

	// 在串行执行中，所有节点都在关键路径上
	totalDuration := time.Duration(0)
	if len(sortedNodes) > 0 {
		totalDuration = sortedNodes[len(sortedNodes)-1].Finish.Sub(sortedNodes[0].Start)
	}

	return sortedNodes, totalDuration
}

// calculateParallelExecutionPath 计算并行执行的关键路径
func (g *Graphviz) calculateParallelExecutionPath(group *graphGroup) ([]*graphNode, time.Duration) {
	// 构建节点映射和依赖关系
	nodeMap := make(map[string]*graphNode)
	dependents := make(map[string][]*graphNode) // dep -> [nodes that depend on dep]

	for _, node := range group.Nodes {
		nodeMap[node.Node.Name()] = node
	}

	for _, node := range group.Nodes {
		for _, depName := range node.Node.Dependencies() {
			if depNode := nodeMap[depName]; depNode != nil {
				dependents[depName] = append(dependents[depName], node)
			}
		}
	}

	// 使用动态规划计算从每个节点开始的最长路径
	type pathInfo struct {
		path          []*graphNode
		totalDuration time.Duration
	}

	memo := make(map[string]*pathInfo)

	var calculateLongestPath func(string) *pathInfo
	calculateLongestPath = func(nodeName string) *pathInfo {
		if info, exists := memo[nodeName]; exists {
			return info
		}

		node := nodeMap[nodeName]
		if node == nil {
			return &pathInfo{path: []*graphNode{}, totalDuration: 0}
		}

		// 当前节点的基础路径
		currentDuration := node.Finish.Sub(node.Start)
		bestPath := []*graphNode{node}
		maxFollowingDuration := time.Duration(0)

		// 查看所有依赖当前节点的后续节点，找到最长路径
		for _, dependent := range dependents[nodeName] {
			followingInfo := calculateLongestPath(dependent.Node.Name())

			// 计算总的路径时间（包括等待时间）
			waitTime := dependent.Start.Sub(node.Finish)
			if waitTime < 0 {
				waitTime = 0
			}
			totalFollowingTime := waitTime + followingInfo.totalDuration

			if totalFollowingTime > maxFollowingDuration {
				maxFollowingDuration = totalFollowingTime
				bestPath = append([]*graphNode{node}, followingInfo.path...)
			}
		}

		result := &pathInfo{
			path:          bestPath,
			totalDuration: currentDuration + maxFollowingDuration,
		}
		memo[nodeName] = result
		return result
	}

	// 找到所有可能的起始节点（在组内没有前驱的节点）
	var startNodes []*graphNode
	for _, node := range group.Nodes {
		hasInternalDependency := false
		for _, depName := range node.Node.Dependencies() {
			if nodeMap[depName] != nil {
				hasInternalDependency = true
				break
			}
		}
		if !hasInternalDependency {
			startNodes = append(startNodes, node)
		}
	}

	// 从所有起始节点中找到最长路径
	var globalLongestPath []*graphNode
	var globalMaxDuration time.Duration

	for _, startNode := range startNodes {
		pathInfo := calculateLongestPath(startNode.Node.Name())

		// 使用路径的实际端到端时间
		actualEndToEndTime := time.Duration(0)
		if len(pathInfo.path) > 0 {
			actualEndToEndTime = pathInfo.path[len(pathInfo.path)-1].Finish.Sub(pathInfo.path[0].Start)
		}

		if actualEndToEndTime > globalMaxDuration {
			globalMaxDuration = actualEndToEndTime
			globalLongestPath = pathInfo.path
		}
	}

	return globalLongestPath, globalMaxDuration
}

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

// GraphvizMW 返回 graphviz 中间件
func GraphvizMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			// 从 context 中获取 graphviz 实例
			graphviz, ok := ctx.Value(graphvizKey{}).(*Graphviz)
			if !ok || graphviz == nil {
				// 如果没有 graphviz 实例，直接执行下一个中间件
				return next(ctx, req)
			}

			// 获取当前的 group path
			groupPath := GetCurrentGroupPath(ctx)

			start := time.Now()
			resp, err := next(ctx, req)
			finish := time.Now()

			// 记录节点执行信息，包含 group path
			graphviz.record(node, groupPath, start, finish, err)

			return resp, err
		}
	}
}
