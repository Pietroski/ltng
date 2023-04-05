package encapsulator

import prefix_models "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/models/prefix"

type (
	Encapsulator interface {
		Encapsule(prefix_models.Prefix, string) string
	}

	strEncapsulator struct{}
)

func NewEncapsulator() Encapsulator {
	e := &strEncapsulator{}

	return e
}

func (e *strEncapsulator) Encapsule(p prefix_models.Prefix, s string) string {
	return p.String() + s
}
