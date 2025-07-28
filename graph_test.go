package dagpher

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

/*
((First + 3) * 5) + 11
((Second + 3) * 5 * 7) + 13
use case dag will exec:  A -> B -> D 330ms
use case parallel will exec: A -> B or C -> D or E 510ms
use case serial will exec: A -> B 20ms -> C 200ms -> D 300ms -> E 30ms 560ms

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
		So(exeCtx.First, ShouldEqual, 20)
		So(exeCtx.Second, ShouldEqual, 20)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*210)
		So(cost, ShouldBeLessThan, time.Millisecond*219)

		// 等待 D 执行完成
		time.Sleep(time.Millisecond * 130)
		So(exeCtx.First, ShouldEqual, 31)
		So(exeCtx.Second, ShouldEqual, 20)
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
		g1.AddNode(E)

		g2 := NewGroup[*Tuple2]("group2")
		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 2})
		g2.AddNode(A)
		g2.AddNode(B)
		g2.AddNode(C)
		g2.AddNode(D)
		g2.AddNode(E)

		g3 := NewGroup[*Tuple2]("group3").SetMaxGoNum(10)
		A, B, C, D, E = NewCalcNodes(Param{SetDep: true, SetName: 3})
		g3.AddNode(A)
		g3.AddNode(B)
		g3.AddNode(C)
		g3.AddNode(D)
		g3.AddNode(E)

		g4 := NewGroup[*Tuple2]("group4")
		A, B, C, D, E = NewCalcNodes(Param{SetDep: false, SetName: 2})
		g4.AddNode(A)
		g4.AddNode(B)
		g4.AddNode(C)
		g4.AddNode(D)
		g4.AddNode(E)

		graph.AddNode(g1)
		graph.AddNode(g2)
		graph.AddNode(g3)
		g3.AddNode(g4)

		//ctx, graph := mw.GraphvizBuilder("flow").Build(ctx)
		//defer graph.Log(ctx)
		//flow.AddGlobalMW(mw.GraphvizMW())

		now := time.Now()
		err := graph.Exec(ctx, exeCtx)
		So(err, ShouldBeNil)
		So(exeCtx.First, ShouldEqual, 4697)
		So(exeCtx.Second, ShouldEqual, 6711801)
		cost := time.Since(now)
		So(cost, ShouldBeGreaterThanOrEqualTo, time.Millisecond*1450)
		So(cost, ShouldBeLessThan, time.Millisecond*1459)
	})
}
