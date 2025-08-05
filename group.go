package dagpher

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/jakenier/dagpher/executor"
)

type Group[C any] struct {
	name        string
	deps        []string
	globalMws   []Middleware
	nodeMap     map[string]Node[C]
	nodeOptions map[string]*option

	parentNamespace string // The namespace path from parent groups (empty for top-level)
	isTopLevel      bool   // True if this group is a top-level container (Graph/Chain)

	globalSem *semaphore.Weighted
	groupExec *groupExecutor[C]
}

func NewGroup[C any](name string, deps ...string) *Group[C] {
	return &Group[C]{
		name:        name,
		deps:        deps,
		nodeMap:     make(map[string]Node[C]),
		nodeOptions: make(map[string]*option),
	}
}

func (g *Group[C]) AddNode(node Node[C], opts ...Option) {
	if _, exists := g.nodeMap[node.Name()]; exists {
		panic("node with the same name already exists: " + node.Name())
	}

	g.nodeMap[node.Name()] = node
	g.nodeOptions[node.Name()] = getOption(opts...)
}

func (g *Group[C]) AddMiddleware(mws ...Middleware) *Group[C] {
	g.globalMws = append(g.globalMws, mws...)
	return g
}

func (g *Group[C]) SetMaxGoNum(maxGoNum int) *Group[C] {
	g.globalSem = semaphore.NewWeighted(int64(maxGoNum))
	return g
}

// setParentNamespace sets the parent namespace for this group
func (g *Group[C]) setParentNamespace(namespace string) *Group[C] {
	g.parentNamespace = namespace
	return g
}

// getFullName returns the full namespaced name of this group
func (g *Group[C]) getFullName() string {
	if g.parentNamespace == "" {
		return g.name
	}
	return g.parentNamespace + "." + g.name
}

// setTopLevel marks this group as a top-level container (Graph/Chain)
func (g *Group[C]) setTopLevel(isTopLevel bool) *Group[C] {
	g.isTopLevel = isTopLevel
	return g
}

func (g *Group[C]) SetGlobalSem(sem *semaphore.Weighted) *Group[C] {
	if g.globalSem != nil { // 优先用当前group的
		return g
	}

	g.globalSem = sem
	return g
}

func (g *Group[C]) Dependencies() []string {
	return g.deps
}

func (g *Group[C]) Build() error {
	g.groupExec = newGroupExecutor(g, g.globalSem, g.globalMws...)
	return g.groupExec.Build()
}

func (g *Group[C]) Exec(ctx context.Context, execCtx C) error {
	if g.groupExec == nil {
		err := g.Build()
		if err != nil {
			return err
		}
	}

	return g.groupExec.Execute(ctx, execCtx)
}

func (g *Group[C]) Name() string {
	return g.name
}

type groupExecutor[C any] struct {
	group     *Group[C]
	globalMws []Middleware
	exec      *executor.Engine[C]

	globalSem *semaphore.Weighted
}

func newGroupExecutor[C any](group *Group[C], globalSem *semaphore.Weighted, globalMws ...Middleware) *groupExecutor[C] {
	var opts []executor.Option
	if globalSem != nil {
		opts = append(opts, executor.WithSem(globalSem))
	}
	exec := executor.NewEngine[C](opts...)

	return &groupExecutor[C]{
		group:     group,
		globalMws: globalMws,
		exec:      exec,
		globalSem: globalSem,
	}
}

func (g *groupExecutor[C]) Build() error {
	visited := make(map[*Group[C]]bool)

	var collectAndAddNodes func(group *Group[C]) error
	collectAndAddNodes = func(group *Group[C]) error {
		if visited[group] {
			return nil
		}
		visited[group] = true

		for name, node := range group.nodeMap {
			capturedNode := node
			capturedName := name

			// If the node is a sub-group, create an executor for it and add it as a single node.
			if subGroup, ok := capturedNode.(*Group[C]); ok {
				// Set the namespace for the sub-group
				// Only set namespace if current group has a namespace (not top-level)
				if group.parentNamespace != "" {
					subGroup.setParentNamespace(group.getFullName())
				} else {
					// Current group is top-level, so sub-group's namespace is just the current group name
					subGroup.setParentNamespace(group.name)
				}

				// Create a new executor for the sub-group, 传递全局信号量
				subGroup.globalMws = append(subGroup.globalMws, g.globalMws...)

				var semToUse *semaphore.Weighted
				if subGroup.globalSem != nil {
					semToUse = subGroup.globalSem
				} else {
					semToUse = g.globalSem
				}
				subGroupExec := newGroupExecutor(subGroup, semToUse, subGroup.globalMws...)
				if err := subGroupExec.Build(); err != nil {
					return err
				}

				// Use the original name for sub-group registration (no namespace prefix for groups)
				// The namespace is handled internally within the sub-group
				err := g.exec.AddContainerNode(capturedName, subGroupExec.Execute, capturedNode.Dependencies()...)
				if err != nil {
					return err
				}
			} else {
				// This is a regular node (叶子节点).
				opt := group.nodeOptions[capturedName]
				mws := opt.mergeMws(g.globalMws)

				execNode := func(ctx context.Context, c C) error {
					_, err := ChainMw(mws...)(node, func(ctx context.Context, in any) (out any, err error) {
						realIn, ok := in.(C)
						if !ok {
							return nil, fmt.Errorf("expected input type %T, got %T", c, in)
						}

						err = capturedNode.Exec(ctx, realIn)
						if err != nil {
							return nil, err
						}
						return realIn, nil
					})(ctx, c)
					if err != nil {
						return err
					}
					return nil
				}

				// Generate the namespaced name for the node
				// Top-level containers (Graph/Chain) don't add namespace prefix to their direct nodes
				nodeNameInExecutor := capturedName
				if !group.isTopLevel {
					// This is not a top-level container, add namespace prefix
					if group.parentNamespace != "" {
						// This group has a parent namespace, use full namespaced name
						nodeNameInExecutor = group.getFullName() + "." + capturedName
					} else {
						// This group is the first level below top-level, use group name as namespace
						nodeNameInExecutor = group.name + "." + capturedName
					}
				}
				// For top-level containers, use original node name without namespace prefix

				// Register node with the determined name
				if err := g.exec.AddNode(nodeNameInExecutor, execNode, capturedNode.Dependencies()...); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := collectAndAddNodes(g.group); err != nil {
		return err
	}

	return g.exec.Build()
}

func (g *groupExecutor[C]) Execute(ctx context.Context, execCtx C) error {
	if g.exec == nil {
		return fmt.Errorf("executor is not built, call Build() first")
	}

	return g.exec.Execute(ctx, execCtx)
}
