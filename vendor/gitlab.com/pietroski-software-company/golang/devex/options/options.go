package options

// Option defines the function signature for configuration options.
type Option func(interface{})

// ApplyOptions applies a list of options to a configuration.
func ApplyOptions(cfg interface{}, opts ...Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}
