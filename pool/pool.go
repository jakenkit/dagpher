package pool

import (
	"context"
	"sync"
)

// Pool is a simple worker pool.
type Pool interface {
	Submit(task func())
	Stop()
}

type workerPool struct {
	maxWorkers int
	tasks      chan func()
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// New creates a new worker pool.
func New(maxWorkers int) Pool {
	if maxWorkers <= 0 {
		maxWorkers = 1 // Ensure at least one worker
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &workerPool{
		maxWorkers: maxWorkers,
		tasks:      make(chan func()),
		cancel:     cancel,
	}
	p.start(ctx)
	return p
}

func (p *workerPool) start(ctx context.Context) {
	p.wg.Add(p.maxWorkers)
	for i := 0; i < p.maxWorkers; i++ {
		go func() {
			defer p.wg.Done()
			for {
				select {
				case task, ok := <-p.tasks:
					if !ok {
						return
					}
					task()
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

// Submit submits a task to the pool.
func (p *workerPool) Submit(task func()) {
	p.tasks <- task
}

// Stop stops the worker pool and waits for all tasks to complete.
func (p *workerPool) Stop() {
	p.cancel()
	close(p.tasks)
	p.wg.Wait()
}
