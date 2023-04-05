package env_converter

import (
	"reflect"
	"strings"
)

func stringSlicer(str string, tag reflect.StructTag) ([]string, error) {
	separator := ","
	if v, ok := tag.Lookup("split"); ok {
		separator = v
	}

	return strings.Split(str, separator), nil
}
