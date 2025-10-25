package env

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gitlab.com/pietroski-software-company/golang/devex/validator"
)

func stringConverter(str string, pt reflect.Kind, pv reflect.Value, tag reflect.StructTag) (any, error) {
	switch pt {
	case reflect.Int:
		return strconv.Atoi(str)
	case reflect.Int32:
		val, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, err
		}

		return int32(val), nil
	case reflect.Int64:
		switch pv.Interface().(type) {
		case time.Duration:
			return time.ParseDuration(str)
		}

		return strconv.ParseInt(str, 10, 64)
	case reflect.Uint32:
		val, err := strconv.ParseUint(str, 10, 32)
		if err != nil {
			return nil, err
		}

		return uint32(val), nil
	case reflect.Uint64:
		return strconv.ParseUint(str, 10, 64)
	case reflect.Float64:
		return strconv.ParseFloat(str, 64)
	case reflect.Bool:
		return strconv.ParseBool(str)
	case reflect.Slice:
		switch pv.Interface().(type) {
		case []string:
			return stringSlicer(str, tag), nil
		}

		fallthrough
	case reflect.Struct:
		switch pv.Interface().(type) {
		case time.Time:
			return validator.TimeParser(str)
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

func stringSlicer(str string, tag reflect.StructTag) []string {
	separator := ","
	if v, ok := tag.Lookup("split"); ok {
		separator = v
	}

	return strings.Split(str, separator)
}
