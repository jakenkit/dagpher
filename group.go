package dagpher

import (
	"context"
	"fmt"

	"github.com/jakenier/dagpher/executor"
	"github.com/jakenier/dagpher/pool"
)

type Group[C any] struct {
	name        string
	deps        []string
	globalMws   []Middleware
	nodeMap     map[string]Node[C]
	nodeOptions map[string]*option
	pool        pool.Pool

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

func (g *Group[C]) SetPool(p pool.Pool) *Group[C] {
	g.pool = p
	return g
}

func (g *Group[C]) Dependencies() []string {
	return g.deps
}

func (g *Group[C]) Build() error {
	g.groupExec = newGroupExecutor(g, g.pool, g.globalMws...)
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
	pool      pool.Pool
}

func newGroupExecutor[C any](group *Group[C], p pool.Pool, globalMws ...Middleware) *groupExecutor[C] {
	var opts []executor.Option
	if p != nil {
		opts = append(opts, executor.WithPool(p))
	}

	exec := executor.NewEngine[C](opts...)
	return &groupExecutor[C]{
		group:     group,
		globalMws: globalMws,
		exec:      exec,
		pool:      p,
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
				// Create a new executor for the sub-group.
				subGroupExec := newGroupExecutor(subGroup, subGroup.pool, subGroup.globalMws...)
				if err := subGroupExec.Build(); err != nil {
					return err
				}

				// Add the sub-group as a single node to the parent executor.
				err := g.exec.AddNode(capturedName, subGroupExec.Execute, capturedNode.Dependencies()...)
				if err != nil {
					return err
				}
			} else {
				// This is a regular node.
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
