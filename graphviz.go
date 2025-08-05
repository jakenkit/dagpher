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

// NewGraphvizBuilder 创建新的 Graphviz 构建器
func NewGraphvizBuilder(name string) *graphvizBuilder {
	return newGraphvizBuilder(name)
}

type graphNode struct {
	Node   DependencyNode
	Start  time.Time
	Finish time.Time
	Error  error
	Group  string // 添加Group信息
}

func newGraphNode(node DependencyNode, group string) *graphNode {
	return &graphNode{
		Node:  node,
		Group: group,
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

	// 新增：用于跟踪节点的Group信息
	nodeGroups map[string]string // nodeName -> groupName
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
		nodeGroups: make(map[string]string),
	}
	g.valid.Store(valid)
	return g
}

func (g *Graphviz) record(node DependencyNode, start, finish time.Time, err error) {
	if !g.valid.Load() {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.nodes) >= maxGraphNodeCount {
		g.valid.Store(false)
		return
	}

	// 尝试从context或其他地方获取group信息
	group := "default"
	if groupName, exists := g.nodeGroups[node.Name()]; exists {
		group = groupName
	}

	g.nodes = append(g.nodes, newGraphNode(node, group).
		record(start, finish, err))
	g.nodeMap[node] = true
}

// SetNodeGroup 设置节点的Group信息
func (g *Graphviz) SetNodeGroup(nodeName, groupName string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nodeGroups[nodeName] = groupName
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

	// 分组 - 现在按照Group信息分组
	groups := g.divideIntoGroupsByGroup()

	// 绘制
	graph := gographviz.NewGraph()
	graphAst, _ := gographviz.Parse([]byte(fmt.Sprintf(`digraph G{rankdir=LR; label="%v %vms";}`,
		g.name, totalCostMs)))
	_ = gographviz.Analyse(graphAst, graph)

	// 绘制子图 - 按Group组织
	clusterIndex := 0
	for groupName, groupNodes := range groups {
		graphName := fmt.Sprintf("cluster_%v", clusterIndex)
		clusterIndex++

		// 计算Group的总耗时
		var minStart, maxFinish time.Time
		if len(groupNodes) > 0 {
			minStart = groupNodes[0].Start
			maxFinish = groupNodes[0].Finish
			for _, node := range groupNodes {
				if node.Start.Before(minStart) {
					minStart = node.Start
				}
				if node.Finish.After(maxFinish) {
					maxFinish = node.Finish
				}
			}
		}

		_ = graph.AddSubGraph("G", graphName, map[string]string{
			"label": fmt.Sprintf(`"Group: %s (%vms)"`, groupName, maxFinish.Sub(minStart).Milliseconds()),
			"style": "solid",
		})

		// 计算最长路径
		longestNode := g.findLongestNodeInGroup(groupNodes)
		longestMap := map[DependencyNode]bool{}
		if longestNode != nil {
			longestMap[longestNode.Node] = true
		}

		// 点
		for _, node := range groupNodes {
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
						str += "Longest\n"
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
					attr["label"] = buildLabel(true)
				}
			}
			_ = graph.AddNode(graphName, name, attr)
		}

		// 边 - 只绘制Group内的边
		for _, node := range groupNodes {
			for _, depName := range node.Node.Dependencies() {
				// 找到对应的依赖节点
				var depNode DependencyNode
				for _, n := range g.nodes {
					if n.Node.Name() == depName {
						depNode = n.Node
						// 只绘制同Group内的依赖边
						if n.Group == groupName {
							break
						}
						depNode = nil
					}
				}
				if depNode == nil || !g.nodeMap[depNode] {
					continue
				}

				attr := map[string]string{}
				if longestMap[node.Node] && longestMap[depNode] {
					attr["color"] = "red"
					attr["label"] = fmt.Sprintf("\"Longest\"")
				}
				_ = graph.AddEdge(nodeNames[depNode], nodeNames[node.Node], true, attr)
			}
		}
	}

	// 绘制跨Group的依赖边
	g.addCrossGroupEdges(graph, groups, nodeNames)

	var (
		path     = g.calculateLongestPath(groups, nodeNames)
		graphUrl = g.compressGraphUrl(fmt.Sprintf("https://dreampuf.github.io/GraphvizOnline/?presentation#%v",
			url.PathEscape(graph.String())))
	)
	return fmt.Sprintf("total cost: %vms, longest path: %v, graph: %v", totalCostMs, path, graphUrl)
}

// 按Group分组
func (g *Graphviz) divideIntoGroupsByGroup() map[string][]*graphNode {
	groups := make(map[string][]*graphNode)

	for _, node := range g.nodes {
		groupName := node.Group
		if groupName == "" {
			groupName = "default"
		}
		groups[groupName] = append(groups[groupName], node)
	}

	return groups
}

// 找到Group中耗时最长的节点
func (g *Graphviz) findLongestNodeInGroup(nodes []*graphNode) *graphNode {
	if len(nodes) == 0 {
		return nil
	}

	longest := nodes[0]
	maxDuration := longest.Finish.Sub(longest.Start)

	for _, node := range nodes {
		duration := node.Finish.Sub(node.Start)
		if duration > maxDuration {
			maxDuration = duration
			longest = node
		}
	}

	return longest
}

// 添加跨Group的依赖边
func (g *Graphviz) addCrossGroupEdges(graph *gographviz.Graph, groups map[string][]*graphNode, nodeNames map[DependencyNode]string) {
	for _, nodes := range groups {
		for _, node := range nodes {
			for _, depName := range node.Node.Dependencies() {
				// 找到依赖节点
				var depNode *graphNode
				for _, n := range g.nodes {
					if n.Node.Name() == depName && n.Group != node.Group {
						depNode = n
						break
					}
				}

				if depNode != nil {
					// 这是跨Group的依赖
					_ = graph.AddEdge(nodeNames[depNode.Node], nodeNames[node.Node], true, map[string]string{
						"style": "dashed",
						"color": "blue",
						"label": "\"cross-group\"",
					})
				}
			}
		}
	}
}

// 计算最长路径
func (g *Graphviz) calculateLongestPath(groups map[string][]*graphNode, nodeNames map[DependencyNode]string) string {
	var pathParts []string

	for groupName, nodes := range groups {
		longest := g.findLongestNodeInGroup(nodes)
		if longest != nil {
			duration := longest.Finish.Sub(longest.Start)
			pathParts = append(pathParts, fmt.Sprintf("[%s:%s(%vms)]",
				groupName, nodeNames[longest.Node], duration.Milliseconds()))
		}
	}

	return strings.Join(pathParts, " -> ")
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

// GraphvizMW 返回 graphviz 中间件 - 更新以支持Group信息
func GraphvizMW() Middleware {
	return func(node DependencyNode, next Endpoint) Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			// 从 context 中获取 graphviz 实例
			graphviz, ok := ctx.Value(graphvizKey{}).(*Graphviz)
			if !ok || graphviz == nil {
				// 如果没有 graphviz 实例，直接执行下一个中间件
				return next(ctx, req)
			}

			start := time.Now()
			resp, err := next(ctx, req)
			finish := time.Now()

			// 记录节点执行信息
			graphviz.record(node, start, finish, err)

			return resp, err
		}
	}
}
