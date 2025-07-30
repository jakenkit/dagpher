package dagpher

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"
)

// Chain represents a sequential executor that executes nodes in append order
type Chain[C any] struct {
	name      string
	built     bool
	nodes     []Node[C]
	globalMws []Middleware
	globalSem *semaphore.Weighted
}

// NewChain creates a new chain executor
func NewChain[C any](name string) *Chain[C] {
	return &Chain[C]{
		name:  name,
		nodes: make([]Node[C], 0),
	}
}

// AddNode adds a node to the chain (can be Node or Group)
func (c *Chain[C]) AddNode(node Node[C]) *Chain[C] {
	c.nodes = append(c.nodes, node)
	return c
}

// SetMaxGoNum sets the maximum number of concurrent goroutines
func (c *Chain[C]) SetMaxGoNum(maxGoNum int) *Chain[C] {
	if maxGoNum > 0 {
		c.globalSem = semaphore.NewWeighted(int64(maxGoNum))
	} else {
		c.globalSem = nil
	}
	return c
}

// SetGlobalSem sets the global semaphore (used by parent containers)
func (c *Chain[C]) SetGlobalSem(sem *semaphore.Weighted) {
	c.globalSem = sem
}

// AddGlobalMW adds global middleware to the chain
func (c *Chain[C]) AddGlobalMW(mws ...Middleware) *Chain[C] {
	c.globalMws = append(c.globalMws, mws...)
	return c
}

// Name returns the chain name
func (c *Chain[C]) Name() string {
	return c.name
}

// Dependencies returns empty slice as chain doesn't have explicit dependencies
func (c *Chain[C]) Dependencies() []string {
	return []string{}
}

// Build prepares the chain for execution
func (c *Chain[C]) Build() error {
	for _, node := range c.nodes {
		if subGroup, ok := node.(*Group[C]); ok {
			if subGroup.globalSem == nil && c.globalSem != nil {
				subGroup.SetGlobalSem(c.globalSem)
			}

			subGroup.AddMiddleware(c.globalMws...)

			if err := subGroup.Build(); err != nil {
				return fmt.Errorf("failed to build sub-group %s: %w", subGroup.Name(), err)
			}
		}
	}
	
	c.built = true
	return nil
}

// Exec executes all nodes in the chain sequentially
func (c *Chain[C]) Exec(ctx context.Context, execCtx C) error {
	if !c.built {
		if err := c.Build(); err != nil {
			return err
		}
	}

	for _, node := range c.nodes {
		if err := c.wrapWithSemaphore(func(ctx context.Context, ec C) error {
			return c.executeNode(ctx, ec, node)
		})(ctx, execCtx); err != nil {
			return err
		}
	}
	return nil
}

// wrapWithSemaphore use semaphore to wrap func
func (c *Chain[C]) wrapWithSemaphore(exec func(context.Context, C) error) func(context.Context, C) error {
	return func(ctx context.Context, ec C) error {
		if c.globalSem == nil {
			return exec(ctx, ec)
		}

		if err := c.globalSem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer c.globalSem.Release(1)

		return exec(ctx, ec)
	}
}

// executeNodeWithMiddleware executes a single node with middleware applied
func (c *Chain[C]) executeNode(ctx context.Context, execCtx C, node Node[C]) error {
	if subGroup, ok := node.(*Group[C]); ok {
		return subGroup.Exec(ctx, execCtx)
	}

	_, err := ChainMw(c.globalMws...)(node, func(ctx context.Context, in any) (out any, err error) {
		realIn, ok := in.(C)
		if !ok {
			return nil, fmt.Errorf("expected input type %T, got %T", execCtx, in)
		}

		err = node.Exec(ctx, realIn)
		if err != nil {
			return nil, err
		}
		return realIn, nil
	})(ctx, execCtx)

	return err
}
