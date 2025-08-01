package dagpher

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"

	"github.com/jakenier/dagpher/executor"
)

const (
	DefaultChainName = "chain"
)

// Chain represents a sequential executor that executes nodes in append order
type Chain[C any] struct {
	name      string
	built     bool
	nodes     []Node[C]
	globalMws []Middleware

	globalSem *semaphore.Weighted
	chainExec *chainExecutor[C]
}

// NewChain creates a new chain executor
func NewChain[C any]() *Chain[C] {
	return &Chain[C]{
		name:  DefaultChainName,
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
	if c.chainExec == nil {
		c.chainExec = newChainExecutor(c, c.globalSem, c.globalMws...)
	}
	defer func() { c.built = true }()
	return c.chainExec.Build()
}

// Exec executes all nodes in the chain sequentially
func (c *Chain[C]) Exec(ctx context.Context, execCtx C) error {
	if !c.built {
		if err := c.Build(); err != nil {
			return err
		}
	}

	return c.chainExec.Execute(ctx, execCtx)
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

// chainExecutor is the internal executor for Chain
type chainExecutor[C any] struct {
	chain     *Chain[C]
	globalMws []Middleware
	exec      *executor.Engine[C]
	globalSem *semaphore.Weighted
}

func newChainExecutor[C any](chain *Chain[C], globalSem *semaphore.Weighted, globalMws ...Middleware) *chainExecutor[C] {
	var opts []executor.Option
	if globalSem != nil {
		opts = append(opts, executor.WithSem(globalSem))
	}
	exec := executor.NewEngine[C](opts...)

	return &chainExecutor[C]{
		chain:     chain,
		globalMws: globalMws,
		exec:      exec,
		globalSem: globalSem,
	}
}

func (ce *chainExecutor[C]) Build() error {
	// For chain execution, we need to create a sequential dependency chain
	var prevNodeName string

	for _, node := range ce.chain.nodes {
		capturedNode := node
		nodeName := node.Name()

		// Determine dependencies - each node depends on the previous one
		var deps []string
		if prevNodeName != "" {
			deps = []string{prevNodeName}
		}

		// If the node is a sub-group, create an executor for it
		if subGroup, ok := capturedNode.(*Group[C]); ok {
			// Add global middlewares to sub-group
			subGroup.globalMws = append(subGroup.globalMws, ce.globalMws...)

			// Determine which semaphore to use
			var semToUse *semaphore.Weighted
			if subGroup.globalSem != nil {
				semToUse = subGroup.globalSem
			} else {
				semToUse = ce.globalSem
			}

			subGroupExec := newGroupExecutor(subGroup, semToUse, subGroup.globalMws...)
			if err := subGroupExec.Build(); err != nil {
				return fmt.Errorf("failed to build sub-group %s: %w", subGroup.Name(), err)
			}

			err := ce.exec.AddContainerNode(nodeName, subGroupExec.Execute, deps...)
			if err != nil {
				return fmt.Errorf("failed to add sub-group node %s: %w", nodeName, err)
			}
		} else {
			// This is a regular node (leaf node)
			execNode := func(ctx context.Context, c C) error {
				_, err := ChainMw(ce.globalMws...)(capturedNode, func(ctx context.Context, in any) (out any, err error) {
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

			// Add as a regular node (leaf node that consumes semaphore)
			err := ce.exec.AddNode(nodeName, execNode, deps...)
			if err != nil {
				return fmt.Errorf("failed to add node %s: %w", nodeName, err)
			}
		}

		prevNodeName = nodeName
	}

	return ce.exec.Build()
}

func (ce *chainExecutor[C]) Execute(ctx context.Context, execCtx C) error {
	if ce.exec == nil {
		return fmt.Errorf("executor is not built, call Build() first")
	}

	return ce.exec.Execute(ctx, execCtx)
}
