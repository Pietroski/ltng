package validator

import (
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

const (
	format = "format"
)

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

func TimeParser(str string) (time.Time, error) {
	for _, tf := range timeFormats {
		parsedTime, err := time.Parse(tf, str)
		if err == nil {
			return parsedTime, nil
		}
	}

	return time.Time{}, errorsx.Errorf("unable to parse time - value: %v", str)
}

func TimeConverter(str string, timeFormat string) (string, error) {
	parsedTime, err := TimeParser(str)
	if err != nil {
		return "", errorsx.Wrapf(err, "unable to convert time into format: %s", timeFormat)
	}

	return parsedTime.Format(timeFormat), nil
}

func checkAfterTime(fieldName string, fieldValue any, parameter string) error {
	parsedTime, err := TimeParser(parameter)
	if err != nil {
		return err
	}

	timeValue, ok := fieldValue.(time.Time)
	if !ok {
		return errorsx.Errorf("unable to convert time to time: field: %v - value: %v", fieldName, fieldValue)
	}

	if timeValue.After(parsedTime) {
		return nil
	}

	return errorsx.Errorf("field value %s must be after %s", fieldName, parameter)
}

func checkBeforeTime(fieldName string, fieldValue any, parameter string) error {
	parsedTime, err := TimeParser(parameter)
	if err != nil {
		return err
	}

	timeValue, ok := fieldValue.(time.Time)
	if !ok {
		return errorsx.Errorf("unable to convert time to time: field: %v - value: %v", fieldName, fieldValue)
	}

	if timeValue.Before(parsedTime) {
		return nil
	}

	return errorsx.Errorf("field value %s must be before %s", fieldName, parameter)
}
