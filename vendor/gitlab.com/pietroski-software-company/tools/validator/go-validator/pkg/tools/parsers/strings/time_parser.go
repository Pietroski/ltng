package strings_parser

import (
	"fmt"
	"time"
)

const (
	format = "format"
)

func TimeParser(str string, timeFormat string, ok bool) (time.Time, error) {
	if ok {
		tf, tfOk := timeFormatMap[timeFormat]
		if tfOk {
			return time.Parse(tf, str)
		}

		return time.Parse(timeFormat, str)
	}

	var err error
	var parsedTime time.Time
	for _, tf := range timeFormats {
		parsedTime, err = time.Parse(tf, str)
		if err == nil {
			return parsedTime, err
		}
	}

	return parsedTime, fmt.Errorf("unable to parse time -> value: %v", str)
}

var (
	timeFormats = []string{
		time.RFC3339,
		time.RFC3339Nano,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,

		time.Stamp,
		time.StampMicro,
		time.StampMilli,
		time.StampNano,

		time.UnixDate,

		time.ANSIC,
		time.Kitchen,
		time.Layout,
		time.RubyDate,
	}

	timeFormatMap = map[string]string{
		"RFC3339":     time.RFC3339,
		"RFC3339Nano": time.RFC3339Nano,
		"RFC822":      time.RFC822,
		"RFC822Z":     time.RFC822Z,
		"RFC850":      time.RFC850,
		"RFC1123":     time.RFC1123,
		"RFC1123Z":    time.RFC1123Z,

		"Stamp":      time.Stamp,
		"StampMicro": time.StampMicro,
		"StampMilli": time.StampMilli,
		"StampNano":  time.StampNano,

		"UnixDate": time.UnixDate,

		"ANSIC":    time.ANSIC,
		"Kitchen":  time.Kitchen,
		"Layout":   time.Layout,
		"RubyDate": time.RubyDate,
	}
)
