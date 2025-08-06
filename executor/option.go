package executor

import (
	"fmt"
	"golang.org/x/sync/semaphore"
)

type option struct {
	maxGoNum int // maximum number of goroutines to run concurrently
	sem      *semaphore.Weighted
}

type Option func(*option)

func defaultOption() *option {
	return &option{
		maxGoNum: 100,
	}
}

func getOption(opts ...Option) *option {
	opt := defaultOption()
	for _, o := range opts {
		o(opt)
	}
	return opt
}

// WithMaxGoNum sets the maximum number of concurrent goroutines.
// Panics if maxGoNum is not positive, as this indicates a programming error.
func WithMaxGoNum(maxGoNum int) Option {
	return func(o *option) {
		if maxGoNum <= 0 {
			panic(fmt.Sprintf("WithMaxGoNum: maxGoNum must be greater than 0, got %d", maxGoNum))
		}
		o.maxGoNum = maxGoNum
	}
}

func WithSem(sem *semaphore.Weighted) Option {
	return func(o *option) {
		o.sem = sem
	}
}
