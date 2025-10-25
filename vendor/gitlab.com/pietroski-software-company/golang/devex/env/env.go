package env

import (
	"os"
	"reflect"

	"gitlab.com/pietroski-software-company/golang/devex/validator"
)

const (
	envName = "env"
)

func Load(cfg any) error {
	load(cfg)

	return validator.NewStructValidator().Validate(cfg)
}

func load(cfg any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(cfg))
	reflectType := reflectValue.Type()

	fieldCount := reflectValue.NumField()
	for idx := 0; idx < fieldCount; idx++ {
		fieldType := reflectType.Field(idx)
		if !fieldType.IsExported() {
			continue // skip unexported fields
		}

		fieldValue := reflectValue.Field(idx)

		if fieldValue.Kind() == reflect.Pointer {
			fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
			fieldValue = fieldValue.Elem()
		}
		if fieldValue.Kind() == reflect.Struct {
			load(fieldValue.Addr().Interface())
		}

		tagKey, ok := fieldType.Tag.Lookup(envName)
		if !ok {
			continue
		}
		tagValue, ok := os.LookupEnv(tagKey)
		if !ok {
			continue
		}

		convertedValue, err := stringConverter(
			tagValue,
			fieldValue.Kind(),
			fieldValue,
			fieldType.Tag,
		)
		if err != nil {
			continue
		}

		reflectFieldValue := reflect.ValueOf(convertedValue)
		if reflectFieldValue.CanConvert(fieldValue.Type()) {
			fieldValue.Set(reflectFieldValue)
		}
	}
}
