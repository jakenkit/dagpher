package executor

import (
	"context"
	"errors"
	"sync"
)

// WorkerPool defines the interface for executing tasks concurrently
type WorkerPool interface {
	// Submit submits a task to the worker pool
	Submit(task func()) error
	// SubmitWithContext submits a task with context to the worker pool
	SubmitWithContext(ctx context.Context, task func()) error
	// Close closes the worker pool and waits for all tasks to complete
	Close() error
	// Size returns the maximum number of workers in the pool
	Size() int
}

// DefaultWorkerPool is a simple implementation of WorkerPool
type DefaultWorkerPool struct {
	workers   chan struct{}
	taskQueue chan func()
	wg        sync.WaitGroup
	once      sync.Once
	closed    bool
	mu        sync.RWMutex
}

// NewDefaultWorkerPool creates a new default worker pool with the specified size
func NewDefaultWorkerPool(size int) *DefaultWorkerPool {
	if size <= 0 {
		size = 1
	}
	
	pool := &DefaultWorkerPool{
		workers:   make(chan struct{}, size),
		taskQueue: make(chan func(), size*2), // Buffer to prevent blocking
	}
	
	// Start workers
	for i := 0; i < size; i++ {
		go pool.worker()
	}
	
	return pool
}

func (p *DefaultWorkerPool) worker() {
	for task := range p.taskQueue {
		p.workers <- struct{}{} // Acquire worker slot
		func() {
			defer func() {
				<-p.workers // Release worker slot
				p.wg.Done()
			}()
			task()
		}()
	}
}

func (p *DefaultWorkerPool) Submit(task func()) error {
	return p.SubmitWithContext(context.Background(), task)
}

func (p *DefaultWorkerPool) SubmitWithContext(ctx context.Context, task func()) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPoolClosed
	}
	p.mu.RUnlock()
	
	p.wg.Add(1)
	
	select {
	case p.taskQueue <- task:
		return nil
	case <-ctx.Done():
		p.wg.Done()
		return ctx.Err()
	}
}

func (p *DefaultWorkerPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil
	}
	
	p.closed = true
	close(p.taskQueue)
	p.wg.Wait()
	return nil
}

func (p *DefaultWorkerPool) Size() int {
	return cap(p.workers)
}

var ErrPoolClosed = errors.New("worker pool is closed")
