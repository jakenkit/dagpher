package dagpher

import (
	"context"
	"fmt"

	"github.com/jakenier/dagpher/executor"
)

type Group[C any] struct {
	name        string
	maxGoNum    int
	deps        []string
	globalMws   []Middleware
	nodeMap     map[string]Node[C]
	nodeOptions map[string]*option

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

func (g *Group[C]) Dependencies() []string {
	return g.deps
}

func (g *Group[C]) Build() error {
	g.groupExec = newGroupExecutor(g, g.maxGoNum, g.globalMws...)
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
}

func newGroupExecutor[C any](group *Group[C], maxGoNum int, globalMws ...Middleware) *groupExecutor[C] {
	exec := executor.NewEngine[C]()
	if maxGoNum != 0 {
		exec = executor.NewEngine[C](executor.WithMaxGoNum(maxGoNum))
	}
	return &groupExecutor[C]{
		group:     group,
		globalMws: globalMws,
		exec:      exec,
	}
}

func (g *groupExecutor[C]) Build() error {
	allNodes := make(map[string]Node[C])
	allOptions := make(map[string]*option)
	visited := make(map[*Group[C]]bool)

	var collect func(group *Group[C])
	collect = func(group *Group[C]) {
		if visited[group] {
			return
		}
		visited[group] = true

		for name, node := range group.nodeMap {
			if _, exists := allNodes[name]; exists {
				panic("node with the same name already exists: " + name)
			}
			allNodes[name] = node
			allOptions[name] = group.nodeOptions[name]
		}
	}

	collect(g.group)

	for name, node := range allNodes {
		capturedNode := node
		capturedName := name

		opt := allOptions[capturedName]
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

	return g.exec.Build()
}

func (g *groupExecutor[C]) Execute(ctx context.Context, execCtx C) error {
	if g.exec == nil {
		return fmt.Errorf("executor is not built, call Build() first")
	}

	return g.exec.Execute(ctx, execCtx)
}
