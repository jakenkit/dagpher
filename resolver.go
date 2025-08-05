package dagpher

import (
	"fmt"
	"sort"
)

// 依赖解析器 - 负责解析复杂的依赖关系
type DependencyResolver[C any] struct {
	allNodes    map[string]ExecutableNode[C] // 全局节点映射
	groups      map[string]*Group[C]         // Group映射
	nodeToGroup map[string]string            // 节点到Group的映射

	// 解析结果
	dependencyGraph  map[string][]string // 依赖图：node -> [dependencies]
	reverseDepsGraph map[string][]string // 反向依赖图：node -> [dependents]
}

func NewDependencyResolver[C any]() *DependencyResolver[C] {
	return &DependencyResolver[C]{
		allNodes:         make(map[string]ExecutableNode[C]),
		groups:           make(map[string]*Group[C]),
		nodeToGroup:      make(map[string]string),
		dependencyGraph:  make(map[string][]string),
		reverseDepsGraph: make(map[string][]string),
	}
}

// 添加Group到解析器
func (r *DependencyResolver[C]) AddGroup(group *Group[C]) error {
	if _, exists := r.groups[group.Name()]; exists {
		return fmt.Errorf("group with name %s already exists", group.Name())
	}

	r.groups[group.Name()] = group

	// 收集Group中的所有节点
	return r.collectNodesFromGroup(group)
}

// 从Group中收集节点（包括嵌套Group）
func (r *DependencyResolver[C]) collectNodesFromGroup(group *Group[C]) error {
	// 收集直接节点
	for name, node := range group.GetNodes() {
		if _, exists := r.allNodes[name]; exists {
			return fmt.Errorf("node with name %s already exists", name)
		}
		r.allNodes[name] = node
		r.nodeToGroup[name] = group.Name()
	}

	// 递归收集嵌套Group的节点
	for _, subGroup := range group.subGroups {
		if err := r.collectNodesFromGroup(subGroup); err != nil {
			return err
		}
	}

	return nil
}

// 解析依赖关系
func (r *DependencyResolver[C]) ResolveDependencies() error {
	// 1. 构建基础依赖图
	if err := r.buildBasicDependencyGraph(); err != nil {
		return err
	}

	// 2. 处理Group级别的依赖
	if err := r.resolveGroupDependencies(); err != nil {
		return err
	}

	// 3. 检测循环依赖
	if err := r.detectCycles(); err != nil {
		return err
	}

	// 4. 构建反向依赖图
	r.buildReverseDependencyGraph()

	return nil
}

// 构建基础依赖图
func (r *DependencyResolver[C]) buildBasicDependencyGraph() error {
	for name, node := range r.allNodes {
		deps := node.Dependencies()
		r.dependencyGraph[name] = make([]string, 0, len(deps))

		for _, dep := range deps {
			// 检查依赖的节点是否存在
			if _, exists := r.allNodes[dep]; !exists {
				return fmt.Errorf("node %s depends on non-existent node %s", name, dep)
			}
			r.dependencyGraph[name] = append(r.dependencyGraph[name], dep)
		}
	}
	return nil
}

// 解析Group级别的依赖
func (r *DependencyResolver[C]) resolveGroupDependencies() error {
	for _, group := range r.groups {
		for _, groupDep := range group.Dependencies() {
			// 检查Group依赖是否存在
			depGroup, exists := r.groups[groupDep]
			if !exists {
				return fmt.Errorf("group %s depends on non-existent group %s", group.Name(), groupDep)
			}

			// 让当前Group的入口节点依赖目标Group的出口节点
			entryNodes := group.GetEntryNodes()
			exitNodes := depGroup.GetExitNodes()

			for _, entryNode := range entryNodes {
				for _, exitNode := range exitNodes {
					// 添加跨Group依赖
					r.dependencyGraph[entryNode.Name()] = append(
						r.dependencyGraph[entryNode.Name()],
						exitNode.Name(),
					)
				}
			}
		}
	}
	return nil
}

// 检测循环依赖
func (r *DependencyResolver[C]) detectCycles() error {
	color := make(map[string]int) // 0: 未访问, 1: 正在访问, 2: 已完成
	var path []string

	var dfs func(node string) error
	dfs = func(node string) error {
		if color[node] == 1 {
			// 找到循环依赖
			cycleStart := -1
			for i, n := range path {
				if n == node {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				cycle := append(path[cycleStart:], node)
				return fmt.Errorf("circular dependency detected: %v", cycle)
			}
		}

		if color[node] == 2 {
			return nil // 已访问过
		}

		color[node] = 1
		path = append(path, node)

		for _, dep := range r.dependencyGraph[node] {
			if err := dfs(dep); err != nil {
				return err
			}
		}

		color[node] = 2
		path = path[:len(path)-1]
		return nil
	}

	for node := range r.allNodes {
		if color[node] == 0 {
			if err := dfs(node); err != nil {
				return err
			}
		}
	}

	return nil
}

// 构建反向依赖图
func (r *DependencyResolver[C]) buildReverseDependencyGraph() {
	for node, deps := range r.dependencyGraph {
		for _, dep := range deps {
			r.reverseDepsGraph[dep] = append(r.reverseDepsGraph[dep], node)
		}
	}
}

// 获取拓扑排序结果
func (r *DependencyResolver[C]) GetTopologicalOrder() ([]string, error) {
	inDegree := make(map[string]int)

	// 初始化入度
	for node := range r.allNodes {
		inDegree[node] = len(r.dependencyGraph[node])
	}

	// 找到所有入度为0的节点
	queue := make([]string, 0)
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	result := make([]string, 0, len(r.allNodes))

	for len(queue) > 0 {
		// 按字典序排序，保证结果的确定性
		sort.Strings(queue)

		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// 更新依赖当前节点的其他节点的入度
		for _, dependent := range r.reverseDepsGraph[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(result) != len(r.allNodes) {
		return nil, fmt.Errorf("failed to get topological order, possible circular dependency")
	}

	return result, nil
}

// 获取节点的直接依赖
func (r *DependencyResolver[C]) GetNodeDependencies(nodeName string) []string {
	return r.dependencyGraph[nodeName]
}

// 获取节点的直接被依赖者
func (r *DependencyResolver[C]) GetNodeDependents(nodeName string) []string {
	return r.reverseDepsGraph[nodeName]
}

// 获取所有节点
func (r *DependencyResolver[C]) GetAllNodes() map[string]ExecutableNode[C] {
	return r.allNodes
}

// 获取节点所属的Group
func (r *DependencyResolver[C]) GetNodeGroup(nodeName string) string {
	return r.nodeToGroup[nodeName]
}
