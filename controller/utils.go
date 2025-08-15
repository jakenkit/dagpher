package controller

import "github.com/jakenier/dagpher"

type IDepend interface {
	Name() string
}

type IDependencyNode interface {
	Name() string
	Depends() []IDepend
}

type DependNode struct {
	NodeName string
	Deps     []IDepend
}

func (d *DependNode) Name() string {
	return d.NodeName
}

func (d *DependNode) Depends() []IDepend {
	return d.Deps
}

func (d *DependNode) Dependencies() []string {
	return dagpher.Map(d.Deps, func(dep IDepend) string {
		return dep.Name()
	})
}

func NewDependNode(nm string, deps ...IDepend) IDependencyNode {
	return &DependNode{
		NodeName: nm,
		Deps:     deps,
	}
}

func ToDependencyNode(n IDependencyNode) dagpher.DependencyNode {
	return &DependNode{
		NodeName: n.Name(),
		Deps:     n.Depends(),
	}
}
