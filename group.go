package dagpher

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/jakenier/dagpher/executor"
)

type Group[C any] struct {
	name        string
	maxGoNum    int
	deps        []string
	globalMws   []Middleware
	nodeMap     map[string]Node[C]
	nodeOptions map[string]*option

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
	g.maxGoNum = maxGoNum
	return g
}

func (g *Group[C]) SetGlobalSem(sem *semaphore.Weighted) *Group[C] {
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
	maxGoNum  int64
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
				// Create a new executor for the sub-group, 传递全局信号量
				subGroupExec := newGroupExecutor(subGroup, g.globalSem, subGroup.globalMws...)
				if err := subGroupExec.Build(); err != nil {
					return err
				}

				// Add the sub-group as a single node to the parent executor.
				// 注意：这里使用AddNode而不是AddLeafNode，因为subGroup不是叶子节点
				// subGroup的执行不消耗信号量，因为其内部的叶子节点会消耗信号量
				err := g.exec.AddContainerNode(capturedName, subGroupExec.Execute, capturedNode.Dependencies()...)
				if err != nil {
					return err
				}
			} else {
				// This is a regular node (叶子节点).
				opt := group.nodeOptions[capturedName]
				mws := opt.mergeMws(g.globalMws)

				execNode := func(ctx context.Context, c C) error {
					_, err := Chain(mws...)(func(ctx context.Context, in any) (out any, err error) {
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

				// 使用AddLeafNode为叶子节点添加信号量控制
				// 只有真正执行业务逻辑的叶子节点才会消耗信号量
				err := g.exec.AddNode(capturedName, execNode, capturedNode.Dependencies()...)
				if err != nil {
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
