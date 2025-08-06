// Package executor provides the core DAG execution engine with topological sorting,
// cycle detection, and concurrent execution capabilities.
//
// The Engine type manages node dependencies and executes them in topological order
// while respecting semaphore-based concurrency limits.
package executor