package dagpher

import (
	"context"
	"strings"
)

type hierarchyPathKey struct{}

// HierarchyPath represents the hierarchical path of execution
type HierarchyPath struct {
	path []string
}

// NewHierarchyPath creates a new hierarchy path
func NewHierarchyPath(root string) *HierarchyPath {
	return &HierarchyPath{
		path: []string{root},
	}
}

// Push adds a new level to the hierarchy path
func (h *HierarchyPath) Push(name string) *HierarchyPath {
	newPath := make([]string, len(h.path)+1)
	copy(newPath, h.path)
	newPath[len(h.path)] = name
	return &HierarchyPath{path: newPath}
}

// String returns the full path as a string
func (h *HierarchyPath) String() string {
	return strings.Join(h.path, "/")
}

// GetPath returns the path components
func (h *HierarchyPath) GetPath() []string {
	result := make([]string, len(h.path))
	copy(result, h.path)
	return result
}

func WithPushedHierarchyPath(ctx context.Context, path string) context.Context {
	var hierarchyPath *HierarchyPath
	if existingPath, ok := GetHierarchyPath(ctx); ok {
		hierarchyPath = existingPath.Push(path)
	} else {
		hierarchyPath = NewHierarchyPath(path)
	}
	return WithHierarchyPath(ctx, hierarchyPath)
}

// WithHierarchyPath stores the hierarchy path in context
func WithHierarchyPath(ctx context.Context, path *HierarchyPath) context.Context {
	return context.WithValue(ctx, hierarchyPathKey{}, path)
}

// GetHierarchyPath retrieves the hierarchy path from context
func GetHierarchyPath(ctx context.Context) (*HierarchyPath, bool) {
	path, ok := ctx.Value(hierarchyPathKey{}).(*HierarchyPath)
	return path, ok
}

// GetCurrentGroupPath returns the current group path from context
func GetCurrentGroupPath(ctx context.Context) string {
	if path, ok := GetHierarchyPath(ctx); ok {
		return path.String()
	}
	return ""
}
