package executor

import "github.com/jakenier/dagpher/pool"

type option struct {
	pool pool.Pool
}

type Option func(*option)

func defaultOption() *option {
	return &option{}
}

func getOption(opts ...Option) *option {
	opt := defaultOption()
	for _, o := range opts {
		o(opt)
	}
	return opt
}

func WithPool(p pool.Pool) Option {
	return func(o *option) {
		o.pool = p
	}
}
