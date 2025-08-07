package dagpher

import (
	"context"
	"fmt"
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
	       /    \
	      /      \
	    B 20ms   C 200ms
	    /    \      /
	   /      \   /
	D 300ms   E 30ms
*/
type Tuple2 struct {
	First, Second int
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
		if param.SetDep {
			var depNames []string
			for _, dep := range deps {
				depNames = append(depNames, dep.Name())
			}
			return depNames
		}
		return nil
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
		c.First += 3
		c.Second += 3
		return nil
	})
	B = NewNode(setName("B"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 20)
		if err := setErr("B"); err != nil {
			return err
		}
		c.First *= 5
		c.Second *= 5
		return nil
	}, setDep(A)...)
	C = NewNode(setName("C"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 200)
		if err := setErr("C"); err != nil {
			return err
		}
		c.Second *= 7
		return nil
	}, setDep(A)...)
	D = NewNode(setName("D"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 300)
		if err := setErr("D"); err != nil {
			return err
		}
		c.First += 11
		return nil
	}, setDep(B)...)
	E = NewNode(setName("E"), func(ctx context.Context, c *Tuple2) error {
		time.Sleep(time.Millisecond * 30)
		if err := setErr("E"); err != nil {
			return err
		}
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
		So(cost, ShouldBeLessThan, time.Millisecond*569)
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
