package dyer

import colour_models "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/models/colours"

type (
	Dyer interface {
		Dye(colour colour_models.Colour, str string) string
	}

	strDyer struct {
		Colours colour_models.ColourList
	}
)

func NewDyer() Dyer {
	d := &strDyer{
		Colours: colour_models.CL,
	}

	return d
}

func (d *strDyer) Dye(colour colour_models.Colour, str string) string {
	return colour.Dye(str)
}
