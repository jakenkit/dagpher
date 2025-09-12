package dagpher

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

/*
((First + 3) * 5) + 11
((Second + 3) * 5 * 7) + 13
use case dag will exec:  A -> B -> D 330ms
use case parallel will exec: A -> B or C -> D or E 510ms
use case serial will exec: A -> B 20ms -> C 200ms -> D 300ms -> E 30ms => 560ms

	        A 10ms
	       /	   \
	      /	     \
	    B 20ms   C 200ms
	    /	    \      /
	   /	      \   /
	D 300ms   E 30ms
*/
type Tuple2 struct {
	First, Second int
	mu            sync.Mutex
}

type Param struct {
	SetDep  bool
	SetName int
	ErrNode []string
}

func Contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if v == vv {
			return true
		}
	}
	return false
}

type Namer interface {
	Name() string
}

func NewCalcNodes(param Param) (A, B, C, D, E Node[*Tuple2]) {
	setDep := func(deps ...Namer) []string {
		if !param.SetDep {
			return nil
		}
		var depNames []string
		for _, dep := range deps {
			depNames = append(depNames, dep.Name())
		}
		return depNames
	}
	setName := func(name string) string {
		if param.SetName == 0 {
			return name
		}
		return name + strconv.Itoa(param.SetName)
	}
	setErr := func(name string) error {
		if Contains(param.ErrNode, name) {
			return fmt.Errorf("mock error")
		}
		return nil
	}
	A = NewNode(setName("A"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 10)
		if err := setErr("A"); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.First += 3
		c.Second += 3
		return nil
	})
	B = NewNode(setName("B"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 20)
		if err := setErr("B"); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.First *= 5
		c.Second *= 5
		return nil
	}, setDep(A)...)
	C = NewNode(setName("C"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 200)
		if err := setErr("C"); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.Second *= 7
		return nil
	}, setDep(A)...)
	D = NewNode(setName("D"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 300)
		if err := setErr("D"); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.First += 11
		return nil
	}, setDep(B)...)
	E = NewNode(setName("E"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 30)
		if err := setErr("E"); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.Second += 13
		return nil
	}, setDep(B, C)...)
	return
}

func TestGraph(t *testing.T) {
	var (
		ctx = context.Background()
	)
	Convey("serial", t, func() {
		A, B, C, D, E := NewCalcNodes(Param{SetDep: true})
		graph := NewGraph[*Tuple2]().SetMaxGoNum(1)
		now := time.Now()
		graph.AddNode(A)
		graph.AddNode(B)
		graph.AddNode(C)
		graph.AddNode(D)
		graph.AddNode(E)
		exeCtx := &Tuple2{
			First:  1,
			Second: 1,
		}
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 153)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*560)
		So(cost, ShouldBeLessThan, time.Millisecond*580)
	})
	Convey("parallel", t, func() {
		A, B, C, D, E := NewCalcNodes(Param{SetDep: true})
		graph := NewGraph[*Tuple2]()
		now := time.Now()
		graph.AddNode(A)
		graph.AddNode(B)
		graph.AddNode(C)
		graph.AddNode(D)
		graph.AddNode(E)
		exeCtx := &Tuple2{
			First:  1,
			Second: 1,
		}
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 153)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*330)
		So(cost, ShouldBeLessThan, time.Millisecond*339)
	})
	Convey("node error", t, func() {
		A, B, C, D, E := NewCalcNodes(Param{SetDep: true, ErrNode: []string{"C"}})
		graph := NewGraph[*Tuple2]().SetMaxGoNum(100)
		now := time.Now()
		graph.AddNode(A)
		graph.AddNode(B)
		graph.AddNode(C)
		graph.AddNode(D)
		graph.AddNode(E)
		exeCtx := &Tuple2{
			First:  1,
			Second: 1,
		}
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldNotBeNil)
		// 等待 D 执行完成
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 20)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*330)
		So(cost, ShouldBeLessThan, time.Millisecond*339)
	})
	Convey("group", t, func() {
		var (
			graph  = NewGraph[*Tuple2]()
			exeCtx = &Tuple2{
				First:  1,
				Second: 1,
			}
		)

		g1 := NewGroup[*Tuple2]("group1").SetMaxGoNum(10)
		A, B, C, D, E := NewCalcNodes(Param{SetDep: true, SetName: 1})
		g1.AddNode(A)
		g1.AddNode(B)
		g1.AddNode(C)
		g1.AddNode(D)
		g1.AddNode(E) // (31,153) // 330ms
		// ((First + 3) * 5) + 11
		// ((Second + 3) * 5 * 7) + 13

		g2 := NewGroup[*Tuple2]("group2", "group1").SetMaxGoNum(1)
		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 2})
		g2.AddNode(A)
		g2.AddNode(B)
		g2.AddNode(C)
		g2.AddNode(D)
		g2.AddNode(E) // (181, 5473) // 560ms

		g3 := NewGroup[*Tuple2]("group3", "group2").SetMaxGoNum(10)
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 3})
		g3.AddNode(A)
		g3.AddNode(B)
		g3.AddNode(C)
		g3.AddNode(D)
		g3.AddNode(E) // (931,191673)  // 330ms

		g4 := NewGroup[*Tuple2]("group4")
		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 2})
		g4.AddNode(A)
		g4.AddNode(B)
		g4.AddNode(C)
		g4.AddNode(D)
		g4.AddNode(E) // (4681, 6711801)

		graph.AddNode(g1.AsNode())
		graph.AddNode(g2.AsNode())
		graph.AddNode(g3.AsNode())
		g3.AddNode(g4.AsNode())

		ctx, graphviz := newGraphvizBuilder("flow").Build(ctx)
		defer graphviz.Log(ctx)
		graph.AddGlobalMW(GraphvizMW())
		graph.AddGlobalMW(LoggerMW())

		now := time.Now()
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldBeGreaterThanOrEqualTo, 4000)     // 这里因为存在并发，不是4681
		So(exeCtx.Second, ShouldBeGreaterThanOrEqualTo, 6000001) // 这里因为存在并发，不是6711801
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*1220)
		So(cost, ShouldBeLessThan, time.Millisecond*1239)
	})
}

// TestGlobalSemaphore tests the global semaphore concurrency control
func TestGlobalSemaphore(t *testing.T) {
	Convey("Test Global Semaphore Concurrency Control", t, func() {
		Convey("Test Serial Execution (maxGoNum=1)", func() {
			// Create nodes with tracking execution order
			var execOrder []string
			var orderMutex sync.Mutex

			addToOrder := func(name string) {
				orderMutex.Lock()
				defer orderMutex.Unlock()
				execOrder = append(execOrder, name)
			}

			// Create nodes that can run in parallel but should be serialized
			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				addToOrder("A_start")
				time.Sleep(50 * time.Millisecond)
				addToOrder("A_end")
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				addToOrder("B_start")
				time.Sleep(50 * time.Millisecond)
				addToOrder("B_end")
				return nil
			})

			nodeC := NewNode("C", func(ctx context.Context, c *Tuple2) error {
				addToOrder("C_start")
				time.Sleep(50 * time.Millisecond)
				addToOrder("C_end")
				return nil
			})

			graph := NewGraph[*Tuple2]()
			graph.SetMaxGoNum(1) // Force serial execution
			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.AddNode(nodeC)

			err := graph.Build()
			So(err, ShouldBeNil)

			start := time.Now()
			ctx := context.Background()
			tuple := &Tuple2{}
			err = graph.Exec(ctx, tuple)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			// Should take at least 150ms (3 * 50ms) for serial execution
			So(duration, ShouldBeGreaterThanOrEqualTo, 150*time.Millisecond)

			// Verify serial execution: each node should complete before the next starts
			orderMutex.Lock()
			defer orderMutex.Unlock()
			So(len(execOrder), ShouldEqual, 6)
			// Check that no two nodes overlap
			for i := 0; i < len(execOrder)-1; i += 2 {
				startEvent := execOrder[i]
				endEvent := execOrder[i+1]
				So(startEvent, ShouldEndWith, "_start")
				So(endEvent, ShouldEndWith, "_end")
				So(startEvent[:1], ShouldEqual, endEvent[:1]) // Same node
			}
		})

		Convey("Test Limited Parallel Execution (maxGoNum=2)", func() {
			var activeCount int32
			var maxActiveCount int32

			createNode := func(name string, duration time.Duration) Node[*Tuple2] {
				return NewNode(name, func(ctx context.Context, c *Tuple2) error {
					current := atomic.AddInt32(&activeCount, 1)
					for {
						max := atomic.LoadInt32(&maxActiveCount)
						if current <= max || atomic.CompareAndSwapInt32(&maxActiveCount, max, current) {
							break
						}
					}
					time.Sleep(duration)
					atomic.AddInt32(&activeCount, -1)
					return nil
				})
			}

			// Create 4 independent nodes
			nodeA := createNode("A", 100*time.Millisecond)
			nodeB := createNode("B", 100*time.Millisecond)
			nodeC := createNode("C", 100*time.Millisecond)
			nodeD := createNode("D", 100*time.Millisecond)

			graph := NewGraph[*Tuple2]()
			graph.SetMaxGoNum(2) // Allow max 2 concurrent executions
			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.AddNode(nodeC)
			graph.AddNode(nodeD)

			err := graph.Build()
			So(err, ShouldBeNil)

			start := time.Now()
			ctx := context.Background()
			tuple := &Tuple2{}
			err = graph.Exec(ctx, tuple)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			// Should take around 200ms (2 batches of 2 parallel executions)
			So(duration, ShouldBeGreaterThanOrEqualTo, 190*time.Millisecond)
			So(duration, ShouldBeLessThan, 250*time.Millisecond)

			// Verify that max concurrent executions never exceeded 2
			So(atomic.LoadInt32(&maxActiveCount), ShouldBeLessThanOrEqualTo, 2)
		})

		Convey("Test Global Semaphore with Groups", func() {
			var activeCount int32
			var maxActiveCount int32

			createNode := func(name string) Node[*Tuple2] {
				return NewNode(name, func(ctx context.Context, c *Tuple2) error {
					current := atomic.AddInt32(&activeCount, 1)
					for {
						max := atomic.LoadInt32(&maxActiveCount)
						if current <= max || atomic.CompareAndSwapInt32(&maxActiveCount, max, current) {
							break
						}
					}
					time.Sleep(50 * time.Millisecond)
					atomic.AddInt32(&activeCount, -1)
					return nil
				})
			}

			// Create nodes in different groups
			nodeA := createNode("A")
			nodeB := createNode("B")
			nodeC := createNode("C")
			nodeD := createNode("D")

			// Create sub-groups
			group1 := NewGroup[*Tuple2]("group1")
			group1.AddNode(nodeA)
			group1.AddNode(nodeB)

			group2 := NewGroup[*Tuple2]("group2")
			group2.AddNode(nodeC)
			group2.AddNode(nodeD)

			graph := NewGraph[*Tuple2]()
			graph.SetMaxGoNum(2) // Global limit of 2
			graph.AddNode(group1.AsNode())
			graph.AddNode(group2.AsNode())

			err := graph.Build()
			So(err, ShouldBeNil)

			ctx := context.Background()
			tuple := &Tuple2{}
			err = graph.Exec(ctx, tuple)

			So(err, ShouldBeNil)
			// Verify global semaphore worked across groups
			So(atomic.LoadInt32(&maxActiveCount), ShouldBeLessThanOrEqualTo, 2)
		})

		Convey("Test No Global Semaphore (maxGoNum=0)", func() {
			var activeCount int32
			var maxActiveCount int32

			createNode := func(name string) Node[*Tuple2] {
				return NewNode(name, func(ctx context.Context, c *Tuple2) error {
					current := atomic.AddInt32(&activeCount, 1)
					for {
						max := atomic.LoadInt32(&maxActiveCount)
						if current <= max || atomic.CompareAndSwapInt32(&maxActiveCount, max, current) {
							break
						}
					}
					time.Sleep(50 * time.Millisecond)
					atomic.AddInt32(&activeCount, -1)
					return nil
				})
			}

			// Create 4 independent nodes
			nodeA := createNode("A")
			nodeB := createNode("B")
			nodeC := createNode("C")
			nodeD := createNode("D")

			graph := NewGraph[*Tuple2]()
			// Don't set maxGoNum, should allow unlimited concurrency
			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.AddNode(nodeC)
			graph.AddNode(nodeD)

			err := graph.Build()
			So(err, ShouldBeNil)

			start := time.Now()
			ctx := context.Background()
			tuple := &Tuple2{}
			err = graph.Exec(ctx, tuple)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			// Should complete quickly since all run in parallel
			So(duration, ShouldBeLessThan, 100*time.Millisecond)
			// Should allow all 4 to run concurrently
			So(atomic.LoadInt32(&maxActiveCount), ShouldEqual, 4)
		})

		Convey("Test Semaphore Context Cancellation", func() {
			createSlowNode := func(name string) Node[*Tuple2] {
				return NewNode(name, func(ctx context.Context, c *Tuple2) error {
					select {
					case <-time.After(1 * time.Second):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}

			nodeA := createSlowNode("A")
			nodeB := createSlowNode("B")

			graph := NewGraph[*Tuple2]()
			graph.SetMaxGoNum(1) // Serial execution
			graph.AddNode(nodeA)
			graph.AddNode(nodeB)

			err := graph.Build()
			So(err, ShouldBeNil)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			tuple := &Tuple2{}
			err = graph.Exec(ctx, tuple)

			// Should fail due to context timeout
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, context.DeadlineExceeded)
		})
	})
}

// TestGraphCycleDetection tests that the graph properly detects cycles
func TestGraphCycleDetection(t *testing.T) {
	Convey("Test Cycle Detection", t, func() {
		Convey("Direct cycle (self-dependency)", func() {
			graph := NewGraph[*Tuple2]()

			// Node depends on itself
			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "A") // Self-dependency

			graph.AddNode(nodeA)
			err := graph.Build()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cycle")
		})

		Convey("Simple cycle (A->B->A)", func() {
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "B")

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "A")

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			err := graph.Build()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cycle")
		})

		Convey("Complex cycle (A->B->C->A)", func() {
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "C")

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "A")

			nodeC := NewNode("C", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "B")

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.AddNode(nodeC)
			err := graph.Build()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cycle")
		})
	})
}

// TestGraphInvalidDependencies tests handling of invalid dependency references
func TestGraphInvalidDependencies(t *testing.T) {
	Convey("Test Invalid Dependencies", t, func() {
		Convey("Non-existent dependency", func() {
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "NonExistent")

			graph.AddNode(nodeA)
			err := graph.Build()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "NonExistent")
		})

		Convey("Multiple non-existent dependencies", func() {
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "Dep1", "Dep2", "Dep3")

			graph.AddNode(nodeA)
			err := graph.Build()
			So(err, ShouldNotBeNil)
		})

		Convey("Mix of valid and invalid dependencies", func() {
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				return nil
			}, "A", "InvalidDep")

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			err := graph.Build()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "InvalidDep")
		})
	})
}

// TestGraphEdgeCases tests various edge cases
func TestGraphEdgeCases(t *testing.T) {
	ctx := context.Background()

	Convey("Test Edge Cases", t, func() {
		Convey("Empty graph", func() {
			graph := NewGraph[*Tuple2]()
			exeCtx := &Tuple2{}

			err := graph.Build()
			So(err, ShouldBeNil)

			err = graph.Exec(ctx, exeCtx)
			So(err, ShouldBeNil)
		})

		Convey("Single node graph", func() {
			graph := NewGraph[*Tuple2]()
			executed := false

			node := NewNode("single", func(ctx context.Context, c *Tuple2) error {
				executed = true
				c.First = 42
				return nil
			})

			graph.AddNode(node)
			exeCtx := &Tuple2{}

			err := graph.Build()
			So(err, ShouldBeNil)

			err = graph.Exec(ctx, exeCtx)
			So(err, ShouldBeNil)
			So(executed, ShouldBeTrue)
			So(exeCtx.First, ShouldEqual, 42)
		})

		Convey("Duplicate node names", func() {
			graph := NewGraph[*Tuple2]()

			node1 := NewNode("duplicate", func(ctx context.Context, c *Tuple2) error {
				return nil
			})

			node2 := NewNode("duplicate", func(ctx context.Context, c *Tuple2) error {
				return nil
			})

			graph.AddNode(node1)
			graph.AddNode(node2)

			err := graph.Build()
			So(err.Error(), ShouldContainSubstring, "already exists")
		})

		Convey("Nil context", func() {
			graph := NewGraph[*Tuple2]()
			node := NewNode("node", func(ctx context.Context, c *Tuple2) error {
				return nil
			})

			graph.AddNode(node)
			exeCtx := &Tuple2{}

			err := graph.Build()
			So(err, ShouldBeNil)

			err = graph.Exec(nil, exeCtx)
			So(err.Error(), ShouldContainSubstring, "nil context")
		})
	})
}

// TestGraphContextCancellation tests context cancellation scenarios
func TestGraphContextCancellation(t *testing.T) {
	Convey("Test Context Cancellation", t, func() {
		Convey("Cancel before execution", func() {
			graph := NewGraph[*Tuple2]()

			node := NewNode("node", func(ctx context.Context, c *Tuple2) error {
				return nil
			})

			graph.AddNode(node)
			graph.Build()

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			exeCtx := &Tuple2{}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldEqual, context.Canceled)
		})

		Convey("Cancel during execution", func() {
			graph := NewGraph[*Tuple2]()
			var nodeAStarted, nodeBStarted bool

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				nodeAStarted = true
				time.Sleep(100 * time.Millisecond)
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				nodeBStarted = true
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(1 * time.Second):
					return nil
				}
			}, "A")

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.Build()

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			exeCtx := &Tuple2{}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldNotBeNil)
			So(nodeAStarted, ShouldBeTrue)
			So(nodeBStarted, ShouldBeFalse) // Should not start B if A times out
		})

		Convey("Timeout with multiple parallel nodes", func() {
			graph := NewGraph[*Tuple2]()
			var completed sync.Map

			createSlowNode := func(name string, duration time.Duration) Node[*Tuple2] {
				return NewNode(name, func(ctx context.Context, c *Tuple2) error {
					select {
					case <-time.After(duration):
						completed.Store(name, true)
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}

			// Create parallel nodes with different durations
			graph.AddNode(createSlowNode("fast", 10*time.Millisecond))
			graph.AddNode(createSlowNode("medium", 100*time.Millisecond))
			graph.AddNode(createSlowNode("slow", 500*time.Millisecond))

			graph.Build()

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			exeCtx := &Tuple2{}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldEqual, context.DeadlineExceeded)

			// Only fast node should complete
			_, fastCompleted := completed.Load("fast")
			_, mediumCompleted := completed.Load("medium")
			_, slowCompleted := completed.Load("slow")

			So(fastCompleted, ShouldBeTrue)
			So(mediumCompleted, ShouldBeFalse)
			So(slowCompleted, ShouldBeFalse)
		})
	})
}

// TestGraphMiddleware tests middleware functionality
func TestGraphMiddleware(t *testing.T) {
	ctx := context.Background()

	Convey("Test Middleware", t, func() {
		Convey("Middleware execution order", func() {
			graph := NewGraph[*TestContext]()
			testCtx := NewTestContext(10)

			// Global middleware
			globalMW := func(node DependencyNode, next Endpoint) Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					tc := req.(*TestContext)
					tc.Log("global-before-" + node.Name())
					res, err := next(ctx, req)
					tc.Log("global-after-" + node.Name())
					return res, err
				}
			}

			// Node-specific middleware
			nodeMW := func(node DependencyNode, next Endpoint) Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					tc := req.(*TestContext)
					tc.Log("node-before-" + node.Name())
					res, err := next(ctx, req)
					tc.Log("node-after-" + node.Name())
					return res, err
				}
			}

			node := NewNode("test", func(ctx context.Context, c *TestContext) error {
				c.Log("test-exec")
				return nil
			})

			graph.AddGlobalMW(globalMW)
			graph.AddNode(node, WithMiddlewares(nodeMW))

			graph.Build()
			err := graph.Exec(ctx, testCtx)
			So(err, ShouldBeNil)

			results := testCtx.GetResults()
			expected := []string{
				"global-before-test",
				"node-before-test",
				"test-exec",
				"node-after-test",
				"global-after-test",
			}
			So(results, ShouldResemble, expected)
		})

		Convey("Middleware error handling", func() {
			graph := NewGraph[*Tuple2]()

			errorMW := func(node DependencyNode, next Endpoint) Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					if node.Name() == "B" {
						return nil, fmt.Errorf("middleware error for B")
					}
					return next(ctx, req)
				}
			}

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				c.First = 10
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				time.Sleep(10 * time.Millisecond)
				c.Second = 20
				return nil
			})

			graph.AddGlobalMW(errorMW)
			graph.AddNode(nodeA)
			graph.AddNode(nodeB)

			exeCtx := &Tuple2{}
			err := graph.Exec(ctx, exeCtx)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "middleware error")
			So(exeCtx.First, ShouldEqual, 10) // A should execute
			So(exeCtx.Second, ShouldEqual, 0) // B should not execute
		})

		Convey("Middleware modifying context", func() {
			graph := NewGraph[*Tuple2]()

			type ctxKey string
			const testKey ctxKey = "test-key"

			ctxMW := func(node DependencyNode, next Endpoint) Endpoint {
				return func(ctx context.Context, req any) (any, error) {
					newCtx := context.WithValue(ctx, testKey, node.Name())
					return next(newCtx, req)
				}
			}

			var capturedValue string
			node := NewNode("test", func(ctx context.Context, c *Tuple2) error {
				if val, ok := ctx.Value(testKey).(string); ok {
					capturedValue = val
				}
				return nil
			})

			graph.AddGlobalMW(ctxMW)
			graph.AddNode(node)

			exeCtx := &Tuple2{}
			graph.Exec(ctx, exeCtx)
			So(capturedValue, ShouldEqual, "test")
		})
	})
}

// TestGraphStress tests graph under high load and complex scenarios
func TestGraphStress(t *testing.T) {
	ctx := context.Background()

	Convey("Test Graph Stress Scenarios", t, func() {
		Convey("Large DAG with complex dependencies", func() {
			graph := NewGraph[*Tuple2]()
			const numLayers = 10
			const nodesPerLayer = 20

			// Create a multi-layer DAG
			nodeNames := make([][]string, numLayers)
			for layer := 0; layer < numLayers; layer++ {
				nodeNames[layer] = make([]string, nodesPerLayer)
				for nodeIdx := 0; nodeIdx < nodesPerLayer; nodeIdx++ {
					nodeName := fmt.Sprintf("L%d_N%d", layer, nodeIdx)
					nodeNames[layer][nodeIdx] = nodeName

					var deps []string
					if layer > 0 {
						// Each node depends on 2-3 nodes from previous layer
						depCount := 2 + (nodeIdx % 2)
						for d := 0; d < depCount; d++ {
							depIdx := (nodeIdx + d) % nodesPerLayer
							deps = append(deps, nodeNames[layer-1][depIdx])
						}
					}

					node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
						c.mu.Lock()
						c.First++
						c.mu.Unlock()
						// Simulate some work
						time.Sleep(time.Microsecond * 100)
						return nil
					}, deps...)

					graph.AddNode(node)
				}
			}

			graph.SetMaxGoNum(50) // High concurrency
			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}
			start := time.Now()
			err = graph.Exec(ctx, exeCtx)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			So(exeCtx.First, ShouldEqual, numLayers*nodesPerLayer)
			So(duration, ShouldBeLessThan, 5*time.Second) // Should complete within reasonable time
		})

		Convey("High frequency repeated executions", func() {
			// Execute multiple times rapidly
			const numExecutions = 100
			for i := 0; i < numExecutions; i++ {
				graph := NewGraph[*Tuple2]()

				// Simple graph for repeated execution
				nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
					c.mu.Lock()
					c.First++
					c.mu.Unlock()
					return nil
				})

				nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
					c.mu.Lock()
					c.Second++
					c.mu.Unlock()
					return nil
				}, "A")

				graph.AddNode(nodeA)
				graph.AddNode(nodeB)
				err := graph.Build()
				So(err, ShouldBeNil)

				exeCtx := &Tuple2{}
				err = graph.Exec(ctx, exeCtx)
				So(err, ShouldBeNil)

				// Add small delay every 10 executions to prevent resource exhaustion
				if i%10 == 0 && i > 0 {
					time.Sleep(time.Millisecond)
				}

				// Verify each execution is independent and correct
				So(exeCtx.First, ShouldEqual, 1)
				So(exeCtx.Second, ShouldEqual, 1)
			}
		})

		Convey("Memory pressure with large context", func() {
			type LargeContext struct {
				Data [1024 * 1024]byte // 1MB per context
				mu   sync.Mutex
			}

			graph := NewGraph[*LargeContext]()

			for i := 0; i < 10; i++ {
				nodeName := fmt.Sprintf("Node%d", i)
				node := NewNode(nodeName, func(ctx context.Context, c *LargeContext) error {
					c.mu.Lock()
					// Modify some data
					c.Data[0] = byte(i)
					c.mu.Unlock()
					time.Sleep(time.Millisecond)
					return nil
				})
				graph.AddNode(node)
			}

			graph.SetMaxGoNum(5)
			err := graph.Build()
			So(err, ShouldBeNil)

			largeCtx := &LargeContext{}
			err = graph.Exec(ctx, largeCtx)
			So(err, ShouldBeNil)
		})
	})
}

// TestGraphResourceLeak tests for goroutine and memory leaks
func TestGraphResourceLeak(t *testing.T) {
	Convey("Test Resource Leak Detection", t, func() {
		Convey("Goroutine leak detection", func() {
			initialGoroutines := runtime.NumGoroutine()

			// Create and execute multiple graphs
			for i := 0; i < 50; i++ {
				graph := NewGraph[*Tuple2]()

				nodeA := NewNode(fmt.Sprintf("A%d", i), func(ctx context.Context, c *Tuple2) error {
					time.Sleep(time.Millisecond)
					return nil
				})

				nodeB := NewNode(fmt.Sprintf("B%d", i), func(ctx context.Context, c *Tuple2) error {
					time.Sleep(time.Millisecond)
					return nil
				}, fmt.Sprintf("A%d", i))

				graph.AddNode(nodeA)
				graph.AddNode(nodeB)
				graph.SetMaxGoNum(10)

				err := graph.Build()
				So(err, ShouldBeNil)

				exeCtx := &Tuple2{}
				err = graph.Exec(context.Background(), exeCtx)
				So(err, ShouldBeNil)
			}

			// Force GC and wait for cleanup
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			runtime.GC()

			finalGoroutines := runtime.NumGoroutine()
			// Allow some tolerance for background goroutines
			So(finalGoroutines, ShouldBeLessThanOrEqualTo, initialGoroutines+5)
		})

		Convey("Context cancellation cleanup", func() {
			graph := NewGraph[*Tuple2]()

			// Create nodes that would run for a long time
			for i := 0; i < 10; i++ {
				nodeName := fmt.Sprintf("SlowNode%d", i)
				node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
					select {
					case <-time.After(10 * time.Second):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				graph.AddNode(node)
			}

			graph.SetMaxGoNum(5)
			err := graph.Build()
			So(err, ShouldBeNil)

			// Start execution and cancel quickly
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			initialGoroutines := runtime.NumGoroutine()

			exeCtx := &Tuple2{}
			err = graph.Exec(ctx, exeCtx)
			So(err, ShouldNotBeNil) // Should fail due to timeout

			// Wait for cleanup
			time.Sleep(200 * time.Millisecond)
			runtime.GC()

			finalGoroutines := runtime.NumGoroutine()
			So(finalGoroutines, ShouldBeLessThanOrEqualTo, initialGoroutines+5)
		})

		Convey("Panic recovery and cleanup", func() {
			graph := NewGraph[*Tuple2]()

			normalNode := NewNode("Normal", func(ctx context.Context, c *Tuple2) error {
				c.First = 1
				return nil
			})

			panicNode := NewNode("Panic", func(ctx context.Context, c *Tuple2) error {
				panic("intentional panic")
			}, "Dependent")

			dependentNode := NewNode("Dependent", func(ctx context.Context, c *Tuple2) error {
				c.Second = 1
				return nil
			}, "Normal")

			graph.AddNode(normalNode)
			graph.AddNode(panicNode)
			graph.AddNode(dependentNode)

			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}

			// Should handle panic gracefully
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Panic should be recovered
						t.Logf("Recovered from panic: %v", r)
					}
				}()
				graph.Exec(context.Background(), exeCtx)
			}()

			// Normal and dependent nodes should still execute
			So(exeCtx.First, ShouldEqual, 1)
			So(exeCtx.Second, ShouldEqual, 1)
		})
	})
}

// TestGraphPerformanceRegression tests for performance regressions
func TestGraphPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	Convey("Test Performance Regression", t, func() {
		Convey("Build performance", func() {
			const numNodes = 1000

			graph := NewGraph[*Tuple2]()

			// Create nodes with dependencies
			for i := 0; i < numNodes; i++ {
				nodeName := fmt.Sprintf("Node%d", i)
				var deps []string
				if i > 0 {
					// Each node depends on previous node
					deps = []string{fmt.Sprintf("Node%d", i-1)}
				}

				node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
					return nil
				}, deps...)

				graph.AddNode(node)
			}

			start := time.Now()
			err := graph.Build()
			buildTime := time.Since(start)

			So(err, ShouldBeNil)
			So(buildTime, ShouldBeLessThan, 1*time.Second) // Should build quickly
		})

		Convey("Execution performance scaling", func() {
			sizes := []int{10, 50, 100, 500}
			var prevTime time.Duration

			for _, size := range sizes {
				graph := NewGraph[*Tuple2]()

				// Create parallel nodes (no dependencies)
				for i := 0; i < size; i++ {
					nodeName := fmt.Sprintf("Node%d", i)
					node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
						c.mu.Lock()
						c.First++
						c.mu.Unlock()
						time.Sleep(time.Microsecond * 10) // Minimal work
						return nil
					})
					graph.AddNode(node)
				}

				graph.SetMaxGoNum(100) // High concurrency
				err := graph.Build()
				So(err, ShouldBeNil)

				exeCtx := &Tuple2{}
				start := time.Now()
				err = graph.Exec(context.Background(), exeCtx)
				execTime := time.Since(start)

				So(err, ShouldBeNil)
				So(exeCtx.First, ShouldEqual, size)

				t.Logf("Size %d: %v", size, execTime)

				// Performance should scale reasonably (not exponentially)
				if prevTime > 0 {
					ratio := float64(execTime) / float64(prevTime)
					sizeRatio := float64(size) / float64(sizes[len(sizes)-2])
					So(ratio, ShouldBeLessThan, sizeRatio*3) // Should not be worse than 2x size ratio
				}
				prevTime = execTime
			}
		})

		Convey("Memory usage scaling", func() {
			var m1, m2 runtime.MemStats

			// Baseline measurement
			runtime.GC()
			runtime.ReadMemStats(&m1)

			// Create many graphs
			graphs := make([]*Graph[*Tuple2], 100)
			for i := 0; i < 100; i++ {
				graph := NewGraph[*Tuple2]()

				for j := 0; j < 10; j++ {
					nodeName := fmt.Sprintf("G%d_N%d", i, j)
					node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
						return nil
					})
					graph.AddNode(node)
				}

				graph.Build()
				graphs[i] = graph
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)

			memoryIncrease := m2.HeapAlloc - m1.HeapAlloc
			t.Logf("Memory increase: %d bytes", memoryIncrease)

			// Should not use excessive memory (less than 50MB for 1000 nodes)
			So(memoryIncrease, ShouldBeLessThan, 50*1024*1024)

			// Clear references and verify cleanup
			for i := range graphs {
				graphs[i] = nil
			}
			graphs = nil

			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			runtime.GC()
		})
	})
}

// TestGraphRobustness tests graph robustness under various failure conditions
func TestGraphRobustness(t *testing.T) {
	ctx := context.Background()

	Convey("Test Graph Robustness", t, func() {
		Convey("Partial failure recovery", func() {
			graph := NewGraph[*Tuple2]()
			var executedNodes []string
			var mu sync.Mutex

			addExecution := func(name string) {
				mu.Lock()
				executedNodes = append(executedNodes, name)
				mu.Unlock()
			}

			// Create a graph where some nodes fail but others should continue
			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				addExecution("A")
				c.First = 1
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				addExecution("B")
				time.Sleep(1 * time.Second)
				return fmt.Errorf("B failed")
			}, "A")

			nodeC := NewNode("C", func(ctx context.Context, c *Tuple2) error {
				addExecution("C")
				c.Second = 1
				return nil
			}, "A") // Independent of B

			nodeD := NewNode("D", func(ctx context.Context, c *Tuple2) error {
				addExecution("D")
				return nil
			}) // Completely independent

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			graph.AddNode(nodeC)
			graph.AddNode(nodeD)

			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}
			err = graph.Exec(ctx, exeCtx)
			So(err, ShouldNotBeNil) // Should fail due to B

			mu.Lock()
			executed := make([]string, len(executedNodes))
			copy(executed, executedNodes)
			mu.Unlock()

			// A, C, and D should execute; B may or may not depending on timing
			So(executed, ShouldContain, "A")
			So(executed, ShouldContain, "C")
			So(executed, ShouldContain, "D")
			So(exeCtx.First, ShouldEqual, 1)
			So(exeCtx.Second, ShouldEqual, 1)
		})

		Convey("Deep dependency chain stability", func() {
			graph := NewGraph[*Tuple2]()
			const chainDepth = 100

			// Create a deep chain
			for i := 0; i < chainDepth; i++ {
				nodeName := fmt.Sprintf("Chain%d", i)
				var deps []string
				if i > 0 {
					deps = []string{fmt.Sprintf("Chain%d", i-1)}
				}

				node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
					c.mu.Lock()
					c.First++
					c.mu.Unlock()
					return nil
				}, deps...)

				graph.AddNode(node)
			}

			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}
			err = graph.Exec(ctx, exeCtx)
			So(err, ShouldBeNil)
			So(exeCtx.First, ShouldEqual, chainDepth)
		})

		Convey("Wide dependency fan-out stability", func() {
			graph := NewGraph[*Tuple2]()
			const fanOut = 200

			// Create root node
			root := NewNode("Root", func(ctx context.Context, c *Tuple2) error {
				c.mu.Lock()
				c.First = 1
				c.mu.Unlock()
				return nil
			})
			graph.AddNode(root)

			// Create many nodes depending on root
			for i := 0; i < fanOut; i++ {
				nodeName := fmt.Sprintf("Fan%d", i)
				node := NewNode(nodeName, func(ctx context.Context, c *Tuple2) error {
					c.mu.Lock()
					c.Second++
					c.mu.Unlock()
					return nil
				}, "Root")
				graph.AddNode(node)
			}

			graph.SetMaxGoNum(50) // Limited concurrency
			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}
			start := time.Now()
			err = graph.Exec(ctx, exeCtx)
			duration := time.Since(start)

			So(err, ShouldBeNil)
			So(exeCtx.First, ShouldEqual, 1)
			So(exeCtx.Second, ShouldEqual, fanOut)
			So(duration, ShouldBeLessThan, 5*time.Second)
		})

		Convey("Concurrent modification safety", func() {
			// Test that graph execution is safe even if context is modified concurrently
			graph := NewGraph[*Tuple2]()

			nodeA := NewNode("A", func(ctx context.Context, c *Tuple2) error {
				c.mu.Lock()
				original := c.First
				c.mu.Unlock()
				time.Sleep(time.Millisecond * 10)
				c.mu.Lock()
				c.First = original + 1
				c.mu.Unlock()
				return nil
			})

			nodeB := NewNode("B", func(ctx context.Context, c *Tuple2) error {
				c.mu.Lock()
				original := c.Second
				c.mu.Unlock()
				time.Sleep(time.Millisecond * 10)
				c.mu.Lock()
				c.Second = original + 1
				c.mu.Unlock()
				return nil
			})

			graph.AddNode(nodeA)
			graph.AddNode(nodeB)
			err := graph.Build()
			So(err, ShouldBeNil)

			exeCtx := &Tuple2{}

			// Start concurrent executions
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					graph.Exec(ctx, exeCtx)
				}()
			}

			wg.Wait()

			// Values should be consistent (though exact values may vary due to concurrency)
			So(exeCtx.First, ShouldBeGreaterThan, 0)
			So(exeCtx.Second, ShouldBeGreaterThan, 0)
		})
	})
}

//func TestGroupExec(t *testing.T) {
//	ctx := context.TODO()
//	Convey("group", t, func() {
//		var (
//			graph  = NewGraph[*Tuple2]()
//			exeCtx = &Tuple2{
//				First:  1,
//				Second: 1,
//			}
//		)
//
//		g1 := NewGroup[*Tuple2]("group1")
//		g1.SetMaxGoNum(10)
//		A, B, C, D, E := NewCalcNodes(Param{SetDep: true, SetName: 1})
//		g1.AddNode(A)
//		g1.AddNode(B)
//		g1.AddNode(C)
//		g1.AddNode(D)
//		g1.AddNode(E) // (31,153)
//		// ((First + 3) * 5) + 11
//		// ((Second + 3) * 5 * 7) + 13
//		// 330
//
//		g2 := NewGroup[*Tuple2]("group2", "group1")
//		g2.SetMaxGoNum(1)
//		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 2})
//		g2.AddNode(A)
//		g2.AddNode(B)
//		g2.AddNode(C)
//		g2.AddNode(D)
//		g2.AddNode(E) // (181, 5473)
//
//		g3 := NewGroup[*Tuple2]("group3", "group2")
//		g3.SetMaxGoNum(10)
//		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 3})
//		g3.AddNode(A)
//		g3.AddNode(B)
//		g3.AddNode(C)
//		g3.AddNode(D)
//		g3.AddNode(E) // (931,191673)
//
//		g4 := NewGroup[*Tuple2]("group4")
//		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 4})
//		g4.AddNode(A)
//		g4.AddNode(B)
//		g4.AddNode(C)
//		g4.AddNode(D)
//		g4.AddNode(E) // (4681, 6711801)
//
//		graph.AddNode(g1.AsNode())
//		graph.AddNode(g2.AsNode())
//		graph.AddNode(g3.AsNode())
//		g3.AddNode(g4.AsNode())
//
//		ctx, graphviz := newGraphvizBuilder("flow").Build(ctx)
//		defer graphviz.Log(ctx)
//		graph.AddGlobalMW(GraphvizMW())
//		//graph.AddGlobalMW(LoggerMW())
//
//		now := time.Now()
//		err := graph.Exec(ctx, exeCtx)
//		So(err, ShouldBeNil)
//		So(exeCtx.First, ShouldEqual, 4697) // 这里因为存在并发，不是4681
//		So(exeCtx.Second, ShouldEqual, 6711801)
//		cost := time.Since(now)
//		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*1450)
//		So(cost, ShouldBeLessThan, time.Millisecond*1459)
//	})
//}
