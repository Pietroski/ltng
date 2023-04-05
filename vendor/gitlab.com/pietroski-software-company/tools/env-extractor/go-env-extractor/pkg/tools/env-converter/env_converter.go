package env_converter

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

const (
	format = "format"
)

func StringConverter(str string, pt reflect.Type, pv reflect.Value, tag reflect.StructTag) (interface{}, error) {
	switch pt.Kind() {
	case reflect.Int:
		return strconv.Atoi(str)
	case reflect.Int32:
		return strconv.ParseInt(str, 10, 32)
	case reflect.Int64:
		switch pv.Interface().(type) {
		case time.Duration:
			return time.ParseDuration(str)
		}

		return strconv.ParseInt(str, 10, 64)
	case reflect.Uint64:
		return strconv.ParseUint(str, 10, 64)
	case reflect.Float64:
		return strconv.ParseFloat(str, 64)
	case reflect.Bool:
		return strconv.ParseBool(str)
	case reflect.Slice:
		switch pv.Interface().(type) {
		case []string:
			return stringSlicer(str, tag)
		}
	case reflect.Struct:
		switch pv.Interface().(type) {
		case time.Time:
			return timeParser(str, tag)
		}

		fallthrough
	case
		reflect.UnsafePointer,
		reflect.Pointer,
		reflect.Map,
		reflect.Array,
		reflect.Chan:
		return str, fmt.Errorf("invalid allowed type")
	}

	return str, nil
}
