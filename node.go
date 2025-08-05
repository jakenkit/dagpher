package dagpher

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// 基础依赖接口
type DependencyNode interface {
	Name() string
	Dependencies() []string
}

// 可执行节点接口 - 真正执行业务逻辑的节点
type ExecutableNode[C any] interface {
	DependencyNode
	Exec(context.Context, C) error
}

// 容器接口 - 用于组织和管理节点
type Container[C any] interface {
	DependencyNode
	GetNodes() map[string]ExecutableNode[C]
	AddNode(node ExecutableNode[C], opts ...Option)
	GetEntryNodes() []ExecutableNode[C] // 获取入口节点（无内部依赖）
	GetExitNodes() []ExecutableNode[C]  // 获取出口节点（无内部被依赖）
}

// Node实现 - 只负责执行具体任务
type Node[C any] struct {
	name string
	deps []string
	exec func(ctx context.Context, c C) error
}

func (n *Node[C]) Name() string {
	return n.name
}

func (n *Node[C]) Dependencies() []string {
	return n.deps
}

func (n *Node[C]) Exec(ctx context.Context, c C) error {
	return n.exec(ctx, c)
}

func NewNode[C any](name string, fn func(ctx context.Context, c C) error, deps ...string) *Node[C] {
	return &Node[C]{
		name: name,
		deps: deps,
		exec: fn,
	}
}

// Group实现 - 只作为容器，不实现ExecutableNode接口
type Group[C any] struct {
	name        string
	deps        []string // Group级别的依赖声明，用于便捷配置
	nodeMap     map[string]ExecutableNode[C]
	nodeOptions map[string]*option
	globalMws   []Middleware
	globalSem   *semaphore.Weighted

	// 嵌套的子Group
	subGroups map[string]*Group[C]
}

func NewGroup[C any](name string, deps ...string) *Group[C] {
	return &Group[C]{
		name:        name,
		deps:        deps,
		nodeMap:     make(map[string]ExecutableNode[C]),
		nodeOptions: make(map[string]*option),
		subGroups:   make(map[string]*Group[C]),
	}
}

func (g *Group[C]) Name() string {
	return g.name
}

func (g *Group[C]) Dependencies() []string {
	return g.deps
}

func (g *Group[C]) GetNodes() map[string]ExecutableNode[C] {
	return g.nodeMap
}

func (g *Group[C]) AddNode(node ExecutableNode[C], opts ...Option) {
	if _, exists := g.nodeMap[node.Name()]; exists {
		panic("node with the same name already exists: " + node.Name())
	}
	g.nodeMap[node.Name()] = node
	g.nodeOptions[node.Name()] = getOption(opts...)
}

func (g *Group[C]) AddGroup(subGroup *Group[C]) {
	if _, exists := g.subGroups[subGroup.Name()]; exists {
		panic("group with the same name already exists: " + subGroup.Name())
	}
	g.subGroups[subGroup.Name()] = subGroup
}

func (g *Group[C]) GetEntryNodes() []ExecutableNode[C] {
	var entryNodes []ExecutableNode[C]

	// 找到没有内部依赖的节点
	internalDeps := make(map[string]bool)
	for _, node := range g.nodeMap {
		for _, dep := range node.Dependencies() {
			if _, exists := g.nodeMap[dep]; exists {
				internalDeps[dep] = true
			}
		}
	}

	for _, node := range g.nodeMap {
		if !internalDeps[node.Name()] {
			entryNodes = append(entryNodes, node)
		}
	}

	return entryNodes
}

func (g *Group[C]) GetExitNodes() []ExecutableNode[C] {
	var exitNodes []ExecutableNode[C]

	// 找到没有内部被依赖的节点
	hasDependents := make(map[string]bool)
	for _, node := range g.nodeMap {
		for _, dep := range node.Dependencies() {
			if _, exists := g.nodeMap[dep]; exists {
				hasDependents[dep] = true
			}
		}
	}

	for _, node := range g.nodeMap {
		if !hasDependents[node.Name()] {
			exitNodes = append(exitNodes, node)
		}
	}

	return exitNodes
}

func (g *Group[C]) AddMiddleware(mws ...Middleware) *Group[C] {
	g.globalMws = append(g.globalMws, mws...)
	return g
}

func (g *Group[C]) SetMaxGoNum(maxGoNum int) *Group[C] {
	g.globalSem = semaphore.NewWeighted(int64(maxGoNum))
	return g
}

func (g *Group[C]) SetGlobalSem(sem *semaphore.Weighted) *Group[C] {
	if g.globalSem != nil {
		return g // 优先使用当前group的
	}
	g.globalSem = sem
	return g
}

// Group级别的便捷操作
func (g *Group[C]) DependsOn(targetGroup *Group[C]) *Group[C] {
	// 让当前Group的所有入口节点依赖目标Group的所有出口节点
	entryNodes := g.GetEntryNodes()
	exitNodes := targetGroup.GetExitNodes()

	for _, entryNode := range entryNodes {
		// 这里需要修改节点的依赖关系
		// 注意：这个实现需要Node支持动态修改依赖
		if nodeImpl, ok := entryNode.(*Node[C]); ok {
			for _, exitNode := range exitNodes {
				nodeImpl.deps = append(nodeImpl.deps, exitNode.Name())
			}
		}
	}

	return g
}
