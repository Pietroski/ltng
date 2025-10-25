package validator

import (
	"fmt"
	"reflect"
	"time"
)

func checkRequirement(fieldName string, fieldValue any, _ string) error {
	ft := reflect.TypeOf(fieldValue).Kind()
	switch ft {
	case reflect.String:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsZero() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}
	case reflect.Map:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsZero() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}
	case
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if isNillable(ft) && fv.IsZero() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		} else if fv.IsZero() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}
		if fv.Len() == 0 {
			return fmt.Errorf(ErrorShouldNotBeEmpty, fieldName)
		}
	case reflect.Pointer:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsNil() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}

		if reflect.TypeOf(fv).Kind() == reflect.Struct {
			switch fieldValue.(type) {
			case time.Time:
				return zeroTimeCheck(fieldName, fieldValue)
			}

			if fv.IsZero() {
				return fmt.Errorf(ErrorShouldNotBeEmpty, fieldName)
			}

			return NewStructValidator().Validate(fv.Interface())
		}
	case reflect.Struct:
		switch fieldValue.(type) {
		case time.Time:
			return zeroTimeCheck(fieldName, fieldValue)
		}

		fv := reflect.ValueOf(fieldValue)
		if fv.IsZero() {
			return fmt.Errorf(ErrorShouldNotBeEmpty, fieldName)
		}

		return NewStructValidator().Validate(fv.Interface())
	}

	return nil
}

func isNillable(kind reflect.Kind) bool {
	// Must be one of these types to be nillable
	return kind == reflect.Ptr ||
		kind == reflect.Interface ||
		kind == reflect.Slice ||
		kind == reflect.Map ||
		kind == reflect.Chan ||
		kind == reflect.Func
}

func zeroTimeCheck(fieldName string, fieldValue interface{}) error {
	tv, ok := fieldValue.(time.Time)
	if ok {
		if tv.IsZero() {
			return fmt.Errorf(ErrorShouldNotBeZero, fieldName)
		}
	}

	return nil
}
