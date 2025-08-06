// Package dagpher provides group management and hierarchical DAG execution.
// This file contains the Group type which allows organizing nodes into hierarchical containers.
package dagpher

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/jakenier/dagpher/executor"
)

// NodeContainer defines the interface for managing nodes within a container.
// It provides methods for adding nodes, configuring middleware, and controlling concurrency.
type NodeContainer[C any] interface {
	AddNode(Node[C], ...Option) error
	AddMiddleware(...Middleware) NodeContainer[C]
	SetMaxGoNum(int) NodeContainer[C]
	SetGlobalSem(*semaphore.Weighted) NodeContainer[C]
}

// GroupNodeAdapter adapts a Group to implement the Node interface
// It's a lightweight adapter that delegates to the group for actual execution
type GroupNodeAdapter[C any] struct {
	name  string
	deps  []string
	group *Group[C]
}

// Name implements DependencyNode interface
func (a *GroupNodeAdapter[C]) Name() string {
	return a.name
}

// Dependencies implements DependencyNode interface
func (a *GroupNodeAdapter[C]) Dependencies() []string {
	return a.deps
}

// Build builds the execution graph for this group node
// Delegates to the underlying group
func (a *GroupNodeAdapter[C]) Build() error {
	return a.group.Build()
}

// Exec implements Node interface by delegating to the group
func (a *GroupNodeAdapter[C]) Exec(ctx context.Context, c C) error {
	return a.group.Exec(ctx, c)
}

// Group manages a collection of nodes as a container
type Group[C any] struct {
	// Node identity (needed for AsNode functionality)
	name string
	deps []string

	// Container management fields
	nodeMap     map[string]Node[C]
	nodeOptions map[string]*option
	globalMws   []Middleware
	globalSem   *semaphore.Weighted

	// Execution state (shared by all adapters)
	groupExec *groupExecutor[C]
	buildOnce sync.Once
	buildErr  error
}

func NewGroup[C any](name string, deps ...string) *Group[C] {
	return &Group[C]{
		name:        name,
		deps:        deps,
		nodeMap:     make(map[string]Node[C]),
		nodeOptions: make(map[string]*option),
	}
}

// AddNode implements NodeContainer interface.
// Returns an error if a node with the same name already exists.
func (g *Group[C]) AddNode(node Node[C], opts ...Option) {
	if _, exists := g.nodeMap[node.Name()]; exists {
		g.buildErr = fmt.Errorf("node with name '%s' already exists in group '%s'", node.Name(), g.name)
		return
	}

	g.nodeMap[node.Name()] = node
	g.nodeOptions[node.Name()] = getOption(opts...)
	return
}

// AddGroup is a convenience method to add another group as a node.
// Returns an error if the group cannot be added.
func (g *Group[C]) AddGroup(subGroup *Group[C], opts ...Option) {
	g.AddNode(subGroup.AsNode(), opts...)
}

// AddMiddleware implements NodeContainer interface
func (g *Group[C]) AddMiddleware(mws ...Middleware) *Group[C] {
	g.globalMws = append(g.globalMws, mws...)
	return g
}

// SetMaxGoNum implements NodeContainer interface
func (g *Group[C]) SetMaxGoNum(maxGoNum int) *Group[C] {
	g.globalSem = semaphore.NewWeighted(int64(maxGoNum))
	return g
}

// SetGlobalSem implements NodeContainer interface
func (g *Group[C]) SetGlobalSem(sem *semaphore.Weighted) *Group[C] {
	if g.globalSem != nil {
		return g // current group's semaphore takes priority
	}

	g.globalSem = sem
	return g
}

// ensureBuilt ensures the group executor is built exactly once in a thread-safe manner
func (g *Group[C]) ensureBuilt() error {
	if g.buildErr != nil {
		return g.buildErr
	}
	g.buildOnce.Do(func() {
		g.groupExec = newGroupExecutor(g, g.globalSem, g.globalMws...)
		g.buildErr = g.groupExec.Build()
	})
	return g.buildErr
}

// Build builds the execution graph for this group
// This method is idempotent and thread-safe
func (g *Group[C]) Build() error {
	return g.ensureBuilt()
}

// Exec executes the group's internal DAG
func (g *Group[C]) Exec(ctx context.Context, execCtx C) error {
	if err := g.ensureBuilt(); err != nil {
		return fmt.Errorf("failed to build group %s: %w", g.name, err)
	}
	return g.groupExec.Execute(ctx, execCtx)
}

// AsNode returns the Node interface for this group
// Creates a new adapter instance each time to avoid circular references
// All adapters share the same build state through the group
func (g *Group[C]) AsNode() Node[C] {
	return &GroupNodeAdapter[C]{
		name:  g.name,
		deps:  g.deps,
		group: g,
	}
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

			// If the node is a GroupNodeAdapter, extract the underlying group
			if adapter, ok := capturedNode.(*GroupNodeAdapter[C]); ok {
				subGroup := adapter.group
				// Create a new executor for the sub-group, propagate global middleware
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

				// Add the sub-group as a container node to the parent executor
				// Container nodes don't consume semaphore slots as their internal nodes will
				err := g.exec.AddContainerNode(capturedName, subGroupExec.Execute, capturedNode.Dependencies()...)
				if err != nil {
					return err
				}
			} else {
				// This is a regular leaf node
				opt := group.nodeOptions[capturedName]
				mws := opt.mergeMws(g.globalMws)

				execNode := func(ctx context.Context, c C) error {
					return ExecuteWithMiddleware(mws, capturedNode, ctx, c)
				}

				// Add leaf node with semaphore control
				// Only actual business logic nodes consume semaphore slots
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
