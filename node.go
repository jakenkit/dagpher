// Package dagpher provides a type-safe DAG execution engine with support for
// hierarchical groups, middleware, and concurrent execution control.
package dagpher

import (
	"context"
)

// DependencyNode represents a node with name and dependencies.
type DependencyNode interface {
	Name() string
	Dependencies() []string
}

// Node represents an executable node in the DAG.
type Node[C any] interface {
	DependencyNode
	Exec(context.Context, C) error
}

// basicNode is the default implementation of Node interface.
// It wraps a user-provided function with dependency information.
type basicNode[C any] struct {
	name string
	deps []string
	exec func(ctx context.Context, c C) error
}

// Dependencies returns the list of dependency names for this node.
func (n *basicNode[C]) Dependencies() []string {
	return n.deps
}

// Exec executes the node's function with the provided context and data.
func (n *basicNode[C]) Exec(ctx context.Context, c C) error {
	return n.exec(ctx, c)
}

// Name returns the node's unique identifier.
func (n *basicNode[C]) Name() string {
	return n.name
}

// NewNode creates a new Node with the given name, execution function, and dependencies.
// The execution function will be called when the node is executed in the DAG.
func NewNode[C any](name string, fn func(ctx context.Context, c C) error, deps ...string) Node[C] {
	return &basicNode[C]{
		name: name,
		deps: deps,
		exec: fn,
	}
}
