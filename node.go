package dagpher

import (
	"context"
)

type DependencyNode interface {
	Name() string
	Dependencies() []string
}

type Node[C any] interface {
	DependencyNode
	Exec(context.Context, C) error
}

type quickNode[C any] struct {
	name string
	deps []string
	exec func(ctx context.Context, c C) error
}

func (q *quickNode[C]) Dependencies() []string {
	return q.deps
}

// Exec implements Node.
func (q *quickNode[C]) Exec(ctx context.Context, c C) error {
	//fmt.Printf("start time: %v, start exec node: %s, exec ctx: %v\n", time.Since(now).Milliseconds(), q.name, c)
	//defer func() {
	//	fmt.Printf("end time: %v, end exec node: %s, exec ctx: %v\n", time.Since(now).Milliseconds(), q.name, c)
	//}()
	return q.exec(ctx, c)
}

// Name implements Node.
func (q *quickNode[C]) Name() string {
	return q.name
}

func NewNode[C any](name string, fn func(ctx context.Context, c C) error, deps ...string) Node[C] {
	return &quickNode[C]{
		name: name,
		deps: deps,
		exec: fn,
	}
}
