package colour_models

import (
	"fmt"
	"runtime"
)

func init() {
	resetter(runtime.GOOS)
}

func resetter(goos string) {
	if goos == "windows" {
		reset = ""
		red = ""
		green = ""
		yellow = ""
		blue = ""
		purple = ""
		cyan = ""
		gray = ""
		white = ""
	}
}

type (
	Colour string

	ColourList struct {
		Reset  Colour
		Red    Colour
		Green  Colour
		Yellow Colour
		Blue   Colour
		Purple Colour
		Cyan   Colour
		Gray   Colour
		White  Colour
	}
)

var (
	reset  Colour = "\033[0m"
	red    Colour = "\033[31m"
	green  Colour = "\033[32m"
	yellow Colour = "\033[33m"
	blue   Colour = "\033[34m"
	purple Colour = "\033[35m"
	cyan   Colour = "\033[36m"
	gray   Colour = "\033[37m"
	white  Colour = "\033[97m"

	CL = ColourList{
		Reset:  reset,
		Red:    red,
		Green:  green,
		Yellow: yellow,
		Blue:   blue,
		Purple: purple,
		Cyan:   cyan,
		Gray:   gray,
		White:  white,
	}
)

func (c Colour) String() string {
	return string(c)
}

// Dye dyes the colour of the given string and returns it.
func (c Colour) Dye(s string) string {
	finalStr := c.dyerStrInterpolation(s)
	return finalStr
}

// dyerStrInterpolation uses string interpolation to mount a new coloured string.
// WARNING - string interpolation showed to be faster than pkg fmt to mount strings.
func (c Colour) dyerStrInterpolation(s string) string {
	finalStr := string(c) + s + string(reset)
	return finalStr
}

// dyerFmt uses package fmt to mount a new coloured string.
// WARNING - string interpolation showed to be faster than pkg fmt to mount strings.
func (c Colour) dyerFmt(s string) string {
	finalStr := fmt.Sprintf("%v%v%v", c, s, reset)
	return finalStr
}
