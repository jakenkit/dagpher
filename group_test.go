package dagpher

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type TestContext struct {
	mu  sync.Mutex
	log []string
}

func (c *TestContext) Log(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, msg)
}

func (c *TestContext) GetLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	logCopy := make([]string, len(c.log))
	copy(logCopy, c.log)
	return logCopy
}

func TestGroupExecution(t *testing.T) {
	t.Run("simple group with one node", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("simple_group_with_one_node")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		group.AddNode(nodeA)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed"}, testCtx.GetLog())
	})

	t.Run("group with dependencies", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("group_with_dependencies")
		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		}, "A")
		group.AddNode(nodeA)
		group.AddNode(nodeB)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed", "B executed"}, testCtx.GetLog())
	})

	t.Run("nested subgroups", func(t *testing.T) {
		testCtx := &TestContext{}
		rootGroup := NewGroup[*TestContext]("root_group")

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		rootGroup.AddNode(nodeA)

		nodeB := NewNode("B", func(ctx context.Context, c *TestContext) error {
			c.Log("B executed")
			return nil
		})
		// A -> subgroup1 -> C
		// subgroup1 contains B
		nodeC := NewNode("C", func(ctx context.Context, c *TestContext) error {
			c.Log("C executed")
			return nil
		}, "subgroup1") // C depends on the whole subgroup1

		subGroup1 := NewGroup[*TestContext]("subgroup1", "A")
		subGroup1.AddNode(nodeB)

		rootGroup.AddNode(subGroup1)
		rootGroup.AddNode(nodeC)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(rootGroup, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		assert.Equal(t, []string{"A executed", "B executed", "C executed"}, testCtx.GetLog())
	})

	t.Run("duplicate node in different groups not panics", func(t *testing.T) {
		rootGroup := NewGroup[*TestContext]("root_group")
		subGroup := NewGroup[*TestContext]("subgroup")

		nodeA1 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })
		nodeA2 := NewNode("A", func(ctx context.Context, c *TestContext) error { return nil })

		rootGroup.AddNode(nodeA1)
		subGroup.AddNode(nodeA2)
		rootGroup.AddNode(subGroup)

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(rootGroup, sem)
		require.NoError(t, executor.Build())
	})

	t.Run("middleware execution order", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("test_group")

		mw1 := func(next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("global mw1 start")
				res, err := next(ctx, req)
				testCtx.Log("global mw1 end")
				return res, err
			}
		}
		mw2 := func(next Endpoint) Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				testCtx.Log("node mw2 start")
				res, err := next(ctx, req)
				testCtx.Log("node mw2 end")
				return res, err
			}
		}

		nodeA := NewNode("A", func(ctx context.Context, c *TestContext) error {
			c.Log("A executed")
			return nil
		})
		group.AddNode(nodeA, WithMiddlewares(mw2))

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem, mw1)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		expectedLog := []string{
			"global mw1 start",
			"node mw2 start",
			"A executed",
			"node mw2 end",
			"global mw1 end",
		}
		assert.Equal(t, expectedLog, testCtx.GetLog())
	})

	t.Run("loop variable capture", func(t *testing.T) {
		testCtx := &TestContext{}
		group := NewGroup[*TestContext]("loop_group")

		// Add multiple nodes to test if the loop variable was captured correctly.
		nodes := []Node[*TestContext]{
			NewNode("A", func(ctx context.Context, c *TestContext) error { c.Log("A"); return nil }),
			NewNode("B", func(ctx context.Context, c *TestContext) error { c.Log("B"); return nil }),
			NewNode("C", func(ctx context.Context, c *TestContext) error { c.Log("C"); return nil }),
		}
		for _, n := range nodes {
			group.AddNode(n)
		}

		sem := semaphore.NewWeighted(1)
		executor := newGroupExecutor(group, sem)
		require.NoError(t, executor.Build())
		require.NoError(t, executor.Execute(context.Background(), testCtx))

		// The order is not guaranteed, so we check for the presence of all logs.
		logs := testCtx.GetLog()
		assert.ElementsMatch(t, []string{"A", "B", "C"}, logs)
	})
}
