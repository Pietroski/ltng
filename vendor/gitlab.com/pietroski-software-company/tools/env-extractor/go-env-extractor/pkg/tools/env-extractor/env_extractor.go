package go_env_extractor

import (
	"os"
	"reflect"

	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"

	env_converter "gitlab.com/pietroski-software-company/tools/env-extractor/go-env-extractor/pkg/tools/env-converter"
)

const (
	envName = "env-name"
)

func LoadEnvs(cfg interface{}) error {
	loadenvs(cfg)

	return validate(cfg)
}

func loadenvs(cfg interface{}) {
	typeOfCfg := reflect.TypeOf(cfg).Elem()
	valueOfCfg := reflect.ValueOf(cfg).Elem()

	limit := typeOfCfg.NumField()

	for idx := 0; idx < limit; idx++ {
		cfgField := typeOfCfg.FieldByIndex([]int{idx})

		if cfgField.Type.Kind() == reflect.Pointer {
			newStructValue := reflect.New(cfgField.Type.Elem())
			valueOfCfg.FieldByName(cfgField.Name).Set(newStructValue)

			loadenvs(valueOfCfg.FieldByName(cfgField.Name).Interface())
		}

		tagName, ok := cfgField.Tag.Lookup(envName)
		if !ok {
			continue
		}

		tagValue, ok := os.LookupEnv(tagName)
		if !ok {
			continue
		}

		structFieldType := valueOfCfg.FieldByName(cfgField.Name).Type()
		cvrtdValue, err := env_converter.StringConverter(
			tagValue,
			structFieldType,
			valueOfCfg.FieldByName(cfgField.Name),
			cfgField.Tag,
		)
		if err != nil {
			continue
		}

		tagValueValue := reflect.ValueOf(cvrtdValue)
		convertedValue := tagValueValue.Convert(structFieldType)
		valueOfCfg.FieldByName(cfgField.Name).Set(convertedValue)

		// TODO: review before deleting
		//tagValueValue := reflect.ValueOf(cvrtdValue)
		//
		//convertedValueType := reflect.TypeOf(cvrtdValue)
		//canConvertType := tagValueValue.CanConvert(convertedValueType)
		//if canConvertType {
		//	convertedValue := tagValueValue.Convert(structFieldType)
		//	valueOfCfg.FieldByName(cfgField.Name).Set(convertedValue)
		//}
	}
}

func validate(cfg interface{}) error {
	return go_validator.NewStructValidator().Validate(cfg)
}
