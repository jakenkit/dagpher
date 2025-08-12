// Package dagpher provides high-level DAG execution interfaces.
// This file contains the Graph type which provides a simple interface for DAG execution.
package dagpher

import (
	"context"

	"golang.org/x/sync/semaphore"
)

const (
	DefaultGraphName = "graph"
)

type Graph[C any] struct {
	globalMws []Middleware
	group     *Group[C]
	maxGoNum  int

	globalSem *semaphore.Weighted
}

func NewGraph[C any]() *Graph[C] {
	return &Graph[C]{
		group: NewGroup[C](DefaultGraphName),
	}
}

func (g *Graph[C]) SetMaxGoNum(maxGoNum int) *Graph[C] {
	g.maxGoNum = maxGoNum

	if maxGoNum > 0 {
		g.globalSem = semaphore.NewWeighted(int64(maxGoNum))
	} else {
		g.globalSem = nil
	}
	g.group.SetGlobalSem(g.globalSem)

	return g
}

func (g *Graph[C]) AddGlobalMW(mws ...Middleware) *Graph[C] {
	g.globalMws = append(g.globalMws, mws...)
	g.group.AddMiddleware(mws...)
	return g
}

// AddNode adds a node to the graph.
// Returns an error if a node with the same name already exists.
func (g *Graph[C]) AddNode(node Node[C], opts ...Option) {
	g.group.AddNode(node, opts...)
}

func (g *Graph[C]) Build() error {
	if g.maxGoNum > 0 && g.globalSem == nil {
		g.globalSem = semaphore.NewWeighted(int64(g.maxGoNum))
	}

	g.group.AddMiddleware(g.globalMws...)
	g.group.SetGlobalSem(g.globalSem)

	return g.group.Build()
}

// Exec executes the graph with hierarchy context initialized
func (g *Graph[C]) Exec(ctx context.Context, execCtx C) error {
	return g.group.Exec(ctx, execCtx)
}
