// Package dagpher provides execution context for tracking group hierarchy.
// This file contains ExecutionContext for providing nodes with group path information.
package dagpher

import (
	"context"
	"strings"
)

const (
	PathSeparator = "/"
)

// ExecutionContextKey is the key for storing ExecutionContext in context.Context
type ExecutionContextKey struct{}

// ExecutionContext provides information about the current execution context,
// including the group path that a node belongs to.
type ExecutionContext struct {
	GroupPath string // e.g., "graph/group1/group2"
	NodeName  string // current node name
}

// NewExecutionContext creates a new ExecutionContext
func NewExecutionContext(groupPath, nodeName string) *ExecutionContext {
	return &ExecutionContext{
		GroupPath: groupPath,
		NodeName:  nodeName,
	}
}

// FullPath returns the full path including the node name
func (ec *ExecutionContext) FullPath() string {
	if ec.GroupPath == "" {
		return ec.NodeName
	}
	return ec.GroupPath + "/" + ec.NodeName
}

// WithExecutionContext adds ExecutionContext to the given context.Context
func WithExecutionContext(ctx context.Context, execCtx *ExecutionContext) context.Context {
	return context.WithValue(ctx, ExecutionContextKey{}, execCtx)
}

// GetExecutionContext retrieves ExecutionContext from context.Context
func GetExecutionContext(ctx context.Context) *ExecutionContext {
	if execCtx, ok := ctx.Value(ExecutionContextKey{}).(*ExecutionContext); ok {
		return execCtx
	}
	return nil
}

// buildGroupPath constructs a group path by joining parent path with current group name
func buildGroupPath(parentPath, groupName string) string {
	if parentPath == "" {
		return groupName
	}
	return parentPath + PathSeparator + groupName
}

// parseGroupPath splits a group path into components
func parseGroupPath(groupPath string) []string {
	if groupPath == "" {
		return []string{}
	}
	return strings.Split(groupPath, PathSeparator)
}

// GetGroupDepth returns the depth of the group path (number of nested groups)
func (ec *ExecutionContext) GetGroupDepth() int {
	return len(parseGroupPath(ec.GroupPath))
}

// GetParentGroupPath returns the parent group path, or empty string if at root
func (ec *ExecutionContext) GetParentGroupPath() string {
	parts := parseGroupPath(ec.GroupPath)
	if len(parts) <= 1 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], PathSeparator)
}

// GetCurrentGroupName returns just the current group name (last component of path)
func (ec *ExecutionContext) GetCurrentGroupName() string {
	parts := parseGroupPath(ec.GroupPath)
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
