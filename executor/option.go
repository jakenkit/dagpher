package executor

import "golang.org/x/sync/semaphore"

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

func WithMaxGoNum(maxGoNum int) Option {
	return func(o *option) {
		if maxGoNum <= 0 {
			panic("maxGoNum must be greater than 0")
		}
		o.maxGoNum = maxGoNum
	}
}

func WithSem(sem *semaphore.Weighted) Option {
	return func(o *option) {
		o.sem = sem
	}
}
