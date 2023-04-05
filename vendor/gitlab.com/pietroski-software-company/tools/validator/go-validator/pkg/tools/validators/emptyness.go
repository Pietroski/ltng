package go_validator

import (
	"fmt"
	"reflect"
	"time"
)

const (
	ErrorRequiredField    = "error: %v is a mandatory field"
	ErrorShouldNotBeEmpty = "error: %v is a mandatory field and should not be empty"
	ErrorShouldNotBeZero  = "error: %v is a mandatory field and should not be a zero value"
)

func checkRequirement(fieldName string, fieldValue interface{}) error {
	switch reflect.TypeOf(fieldValue).Kind() {
	case reflect.String:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsZero() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}
	case
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsNil() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}
		if fv.Len() == 0 {
			return fmt.Errorf(ErrorShouldNotBeEmpty, fieldName)
		}
	case reflect.Ptr:
		fv := reflect.ValueOf(fieldValue)
		if fv.IsNil() {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}

		switch reflect.TypeOf(fv).Kind() {
		case reflect.Struct:
			nfv := fv.Elem().Interface()
			return NewStructValidator().Validate(nfv)
		}
	case reflect.Struct:
		switch fieldValue.(type) {
		case time.Time:
			return zeroTimeCheck(fieldName, fieldValue)
		}

		fv := reflect.ValueOf(fieldValue).Interface()
		return NewStructValidator().Validate(fv)
	}

	return nil
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
