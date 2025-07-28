package executor

type option struct {
	maxGoNum   int        // maximum number of goroutines to run concurrently
	workerPool WorkerPool // global worker pool for controlling concurrency
}

type Option func(*option)

func defaultOption() *option {
	return &option{
		maxGoNum:   100, // default to 100 goroutines
		workerPool: nil, // will be initialized if not provided
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

// WithWorkerPool sets a custom worker pool for controlling global concurrency
func WithWorkerPool(pool WorkerPool) Option {
	return func(o *option) {
		if pool == nil {
			panic("worker pool cannot be nil")
		}
		o.workerPool = pool
	}
}
