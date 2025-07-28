package dagpher

import "context"

const (
	DefaultGraphName = "dagpher_graph"
)

type Graph[C any] struct {
	globalMws []Middleware
	group     *Group[C]
	maxGoNum  int
}

func NewGraph[C any]() *Graph[C] {
	return &Graph[C]{
		group: NewGroup[C](DefaultGraphName),
	}
}

func (g *Graph[C]) SetMaxGoNum(maxGoNum int) *Graph[C] {
	g.maxGoNum = maxGoNum
	return g
}

func (g *Graph[C]) AddGlobalMW(mws ...Middleware) *Graph[C] {
	g.globalMws = append(g.globalMws, mws...)
	return g
}

func (g *Graph[C]) AddNode(node Node[C], opts ...Option) {
	g.group.AddNode(node, opts...)
}

func (g *Graph[C]) Build() error {
	return g.group.Build()
}

func (g *Graph[C]) Exec(ctx context.Context, execCtx C) error {
	return g.group.Exec(ctx, execCtx)
}
