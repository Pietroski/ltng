package validator

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
)

const validationTag = "validation"

type (
	Validator interface {
		Validate(obj any) (err error)
		Use(validatorName string, customValidator CustomValidator) Validator
		UseMap(customValidators CustomValidatorMap) Validator
	}

	validation struct {
		ctx context.Context

		customValidators map[string]CustomValidator
	}

	CustomValidator    func(fieldName string, fieldValue any) error
	CustomValidatorMap map[string]CustomValidator
)

func WithCustomValidator(name string, customValidator CustomValidator) options.Option {
	return func(obj any) {
		if v, ok := obj.(*validation); ok {
			if len(v.customValidators) == 0 {
				v.customValidators = CustomValidatorMap{}
			}

			v.customValidators[name] = customValidator
		}
	}
}

func NewStructValidator(opts ...options.Option) Validator {
	v := &validation{
		ctx: context.Background(),
	}
	options.ApplyOptions(v, opts...)

	return v
}

func (v *validation) Validate(obj any) error {
	return v.validateStruct(obj)
}

func (v *validation) Use(validatorName string, customValidator CustomValidator) Validator {
	if len(v.customValidators) == 0 {
		v.customValidators = CustomValidatorMap{}
	}

	v.customValidators[validatorName] = customValidator

	return v
}

func (v *validation) UseMap(customValidators CustomValidatorMap) Validator {
	customValidatorsLen := len(customValidators)
	if customValidatorsLen == 0 {
		return v
	}

	if len(v.customValidators) == 0 {
		v.customValidators = make(CustomValidatorMap, customValidatorsLen)
	}

	for customValidatorKey, customValidatorVal := range customValidators {
		v.customValidators[customValidatorKey] = customValidatorVal
	}

	return v
}

func splitStrIntoStrMap(s string) map[string]string {
	xs := strings.Split(s, ";")
	ssLen := len(xs)

	mss := make(map[string]string, ssLen)
	for _, v := range xs {
		st := strings.Split(v, "=")
		if len(st) == 2 {
			mss[st[0]] = st[1]
			continue
		}
		mss[st[0]] = ""
	}

	return mss
}

func (v *validation) validateStruct(obj any) error {
	defer func() {
		if r := recover(); r != nil {
			slogx.New().Error(v.ctx, fmt.Sprintf("recovered from panic error: %v", r))
		}
	}()

	if obj == nil {
		return errorsx.Wrapf(errorsx.Wrap(
			errorsx.New(NilObjErrMsg),
			NothingToCheckErrMsg),
			"%#v", obj)
	}

	reflectVal := reflect.Indirect(reflect.ValueOf(obj))
	if reflectVal.IsZero() {
		return errorsx.Wrapf(errorsx.Wrap(
			errorsx.New(EmptyObjErrMsg), NothingToCheckErrMsg),
			"%#v", obj)
	}
	reflectType := reflectVal.Type()

	fieldCount := reflectVal.NumField()
	for idx := 0; idx < fieldCount; idx++ {
		fieldType := reflectType.Field(idx)
		if !fieldType.IsExported() {
			continue // skip unexported fields
		}

		fieldValue := reflect.Indirect(reflectVal.Field(idx))
		fieldName := fieldType.Name

		fieldTag := fieldType.Tag.Get(validationTag)
		fieldValidators := splitStrIntoStrMap(fieldTag)

		for name, parameter := range fieldValidators {
			if validator, ok := checkers[name]; ok {
				if err := validator(fieldName, fieldValue.Interface(), parameter); err != nil {
					return errorsx.Wrapf(err, "failed to validate field %v", fieldName)
				}
			}

			if err := v.checkCustom(fieldName, fieldValue.Interface(), fieldValidators); err != nil {
				return errorsx.Wrapf(err, "failed to custom validate field %v", fieldName)
			}
		}
	}

	return nil
}
