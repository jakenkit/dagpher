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

// contextKey 用于在 context 中传递容器路径信息
type containerPathKey struct{}

type containerPath struct {
	path []string // 从最外层到当前层的容器名称路径
}

func (cp *containerPath) getFullName(nodeName string) string {
	// 过滤掉顶层的 graph 名称（通常是 "dagpher_graph" 或类似的）
	// 以及其他顶层容器，只保留有意义的中间层
	meaningfulPath := make([]string, 0)

	for i, name := range cp.path {
		// 跳过第一层（通常是 dagpher_graph）
		if i == 0 {
			continue
		}
		// 如果是第二层，检查是否是顶层容器（如 chain 或单个 group）
		// 如果只有两层路径，也不添加前缀
		if i == 1 && len(cp.path) == 2 {
			continue
		}
		meaningfulPath = append(meaningfulPath, name)
	}

	if len(meaningfulPath) == 0 {
		return nodeName
	}

	prefix := strings.Join(meaningfulPath, ".")
	return fmt.Sprintf("%s.%s", prefix, nodeName)
}

func (cp *containerPath) getCurrentContainer() string {
	if len(cp.path) == 0 {
		return ""
	}
	return cp.path[len(cp.path)-1]
}

// withContainerPath 在 context 中添加容器路径信息
func withContainerPath(ctx context.Context, containerName string) context.Context {
	if currentPath, ok := ctx.Value(containerPathKey{}).(*containerPath); ok {
		newPath := &containerPath{
			path: append(currentPath.path, containerName),
		}
		return context.WithValue(ctx, containerPathKey{}, newPath)
	}
	return context.WithValue(ctx, containerPathKey{}, &containerPath{
		path: []string{containerName},
	})
}

// getContainerPath 从 context 中获取容器路径信息
func getContainerPath(ctx context.Context) *containerPath {
	if path, ok := ctx.Value(containerPathKey{}).(*containerPath); ok {
		return path
	}
	return &containerPath{path: []string{}}
}

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
	// 同时初始化容器路径，以 graph name 作为根容器
	ctx = withContainerPath(ctx, b.name)
	return context.WithValue(ctx, graphvizKey{}, graph), graph
}

type graphNode struct {
	Node      DependencyNode
	FullName  string // 带容器前缀的完整名称
	Container string // 所属容器名称
	Start     time.Time
	Finish    time.Time
	Error     error
	IsGroup   bool // 标记是否是组节点
}

func newGraphNode(node DependencyNode, fullName, container string, isGroup bool) *graphNode {
	return &graphNode{
		Node:      node,
		FullName:  fullName,
		Container: container,
		IsGroup:   isGroup,
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

// GroupNode 接口用于标识组节点
type GroupNode interface {
	DependencyNode
	IsGroup() bool
}

func (g *Graphviz) record(node DependencyNode, fullName, container string, start, finish time.Time, err error) {
	if !g.valid.Load() {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.nodes) >= maxGraphNodeCount {
		g.valid.Store(false)
		return
	}

	// 检查是否是 Group 类型
	var isGroup bool
	if groupNode, ok := node.(GroupNode); ok {
		isGroup = groupNode.IsGroup()
	} else {
		// 通过反射或其他方式检查类型名称
		typeName := fmt.Sprintf("%T", node)
		isGroup = strings.Contains(typeName, "Group")
	}

	g.nodes = append(g.nodes, newGraphNode(node, fullName, container, isGroup).
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

	// 按容器分组节点
	containerGroups := g.groupByContainer()

	// 绘制
	graph := gographviz.NewGraph()
	graphAst, _ := gographviz.Parse([]byte(fmt.Sprintf(`digraph G{rankdir=LR; label="%v %vms";}`,
		g.name, totalCostMs)))
	_ = gographviz.Analyse(graphAst, graph)

	// 绘制每个容器的子图
	for containerName, nodes := range containerGroups {
		if containerName == g.name || containerName == "" {
			// 顶层容器不单独框出来
			g.addNodesToGraph(graph, "G", nodes, "")
		} else {
			// 非顶层容器创建子图
			subGraphName := fmt.Sprintf("cluster_%s", strings.ReplaceAll(containerName, ".", "_"))
			totalMs := g.calculateGroupTotalTime(nodes)
			_ = graph.AddSubGraph("G", subGraphName, map[string]string{
				"label": fmt.Sprintf(`"%s %vms"`, containerName, totalMs),
				"style": "solid",
			})
			g.addNodesToGraph(graph, subGraphName, nodes, containerName)
		}
	}

	// 添加依赖边
	g.addDependencyEdges(graph, containerGroups)

	// 生成路径信息和URL
	var pathInfo string
	if len(g.nodes) > 0 {
		longestPath := g.findLongestPath()
		pathNames := make([]string, len(longestPath))
		for i, node := range longestPath {
			pathNames[i] = node.FullName
		}
		totalPathCost := longestPath[len(longestPath)-1].Finish.Sub(longestPath[0].Start).Milliseconds()
		pathInfo = fmt.Sprintf("longest path: [%s](%vms)", strings.Join(pathNames, "->"), totalPathCost)
	}

	graphUrl := g.compressGraphUrl(fmt.Sprintf("https://dreampuf.github.io/GraphvizOnline/?presentation#%v",
		url.PathEscape(graph.String())))

	return fmt.Sprintf("total cost: %vms, %s, graph: %v", totalCostMs, pathInfo, graphUrl)
}

// groupByContainer 按容器分组节点
func (g *Graphviz) groupByContainer() map[string][]*graphNode {
	groups := make(map[string][]*graphNode)

	for _, node := range g.nodes {
		container := node.Container
		if container == "" {
			container = g.name
		}
		groups[container] = append(groups[container], node)
	}

	return groups
}

// calculateGroupTotalTime 计算分组的总耗时
func (g *Graphviz) calculateGroupTotalTime(nodes []*graphNode) int64 {
	if len(nodes) == 0 {
		return 0
	}

	minStart := nodes[0].Start
	maxFinish := nodes[0].Finish

	for _, node := range nodes {
		if node.Start.Before(minStart) {
			minStart = node.Start
		}
		if node.Finish.After(maxFinish) {
			maxFinish = node.Finish
		}
	}

	return maxFinish.Sub(minStart).Milliseconds()
}

// addNodesToGraph 添加节点到图中
func (g *Graphviz) addNodesToGraph(graph *gographviz.Graph, parentName string, nodes []*graphNode, containerPrefix string) {
	longestPath := g.findLongestPathInNodes(nodes)
	longestMap := make(map[*graphNode]bool)
	for _, node := range longestPath {
		longestMap[node] = true
	}

	for _, node := range nodes {
		var attr map[string]string
		if node.Error != nil {
			attr = map[string]string{
				"label":     fmt.Sprintf(`"%v\nError"`, node.FullName),
				"style":     "filled",
				"fillcolor": "red",
			}
		} else {
			costMs := node.Finish.Sub(node.Start).Milliseconds()
			labelText := fmt.Sprintf(`"%v\n%vms`, node.FullName, costMs)

			if longestMap[node] {
				labelText += "\nLongest"
			}

			if g.costDetail {
				labelText += fmt.Sprintf("\n[%v,%v]",
					node.Start.Sub(g.start).Milliseconds(),
					node.Finish.Sub(g.start).Milliseconds())
			}
			labelText += `"`

			attr = map[string]string{
				"label": labelText,
				"color": "green",
			}
			if longestMap[node] {
				attr["color"] = "red"
			}
		}

		// 使用节点的完整名称作为图中的节点ID
		nodeId := strings.ReplaceAll(node.FullName, ".", "_")
		_ = graph.AddNode(parentName, nodeId, attr)
	}
}

// addDependencyEdges 添加依赖边
func (g *Graphviz) addDependencyEdges(graph *gographviz.Graph, containerGroups map[string][]*graphNode) {
	// 创建节点名称到完整名称的映射
	nameToFullName := make(map[string]string)
	nodeNameToNode := make(map[string]*graphNode)

	for _, nodes := range containerGroups {
		for _, node := range nodes {
			nameToFullName[node.Node.Name()] = node.FullName
			nodeNameToNode[node.Node.Name()] = node
		}
	}

	// 添加依赖边
	for _, nodes := range containerGroups {
		for _, node := range nodes {
			for _, depName := range node.Node.Dependencies() {
				if fullDepName, exists := nameToFullName[depName]; exists {
					fromId := strings.ReplaceAll(fullDepName, ".", "_")
					toId := strings.ReplaceAll(node.FullName, ".", "_")

					attr := map[string]string{}

					// 检查是否是组间依赖（特殊标记）
					depNode := nodeNameToNode[depName]
					if depNode != nil && depNode.IsGroup && node.IsGroup {
						attr["style"] = "bold"
						attr["color"] = "blue"
						attr["label"] = "group_dep"
					}

					_ = graph.AddEdge(fromId, toId, true, attr)
				}
			}
		}
	}
}

// findLongestPath 查找最长路径
func (g *Graphviz) findLongestPath() []*graphNode {
	return g.findLongestPathInNodes(g.nodes)
}

// findLongestPathInNodes 在给定节点中查找最长路径
func (g *Graphviz) findLongestPathInNodes(nodes []*graphNode) []*graphNode {
	if len(nodes) == 0 {
		return nil
	}

	// 简化实现：返回耗时最长的节点
	longest := nodes[0]
	maxDuration := longest.Finish.Sub(longest.Start)

	for _, node := range nodes {
		duration := node.Finish.Sub(node.Start)
		if duration > maxDuration {
			maxDuration = duration
			longest = node
		}
	}

	return []*graphNode{longest}
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

			// 获取容器路径信息
			containerPath := getContainerPath(ctx)
			fullName := containerPath.getFullName(node.Name())
			container := containerPath.getCurrentContainer()

			start := time.Now()
			resp, err := next(ctx, req)
			finish := time.Now()

			// 记录节点执行信息
			graphviz.record(node, fullName, container, start, finish, err)

			return resp, err
		}
	}
}
