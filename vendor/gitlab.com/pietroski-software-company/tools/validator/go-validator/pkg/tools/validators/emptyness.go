package go_validator

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	ErrorRequiredField    = "error: %v is a mandatory field"
	ErrorShouldNotBeEmpty = "error: %v is a mandatory field and should not be empty"
	ErrorShouldNotBeZero  = "error: %v is a mandatory field and should not be a zero value"
)

var (
	NothingToCheckErr = errors.New("nothing to check")
)

func checkRequirement(fieldName string, fieldValue interface{}) error {
	ft := reflect.TypeOf(fieldValue).Kind()
	switch ft {
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
	case reflect.Pointer:
		// TODO: remove - we want to make pointers optional
		// TODO: therefore, pointer to primitive data structure, cannot be required
		//err := pointerTypeRequirement(fieldName, fieldValue)
		//if err == nil {
		//	return nil
		//} else if err != nil && err != NothingToCheckErr {
		//	return err
		//}

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

func pointerTypeRequirement(fieldName string, fieldValue interface{}) error {
	if fieldValue == nil {
		return fmt.Errorf(ErrorRequiredField, fieldName)
	}

	err := pointerTypeRequirementForStrings(fieldName, fieldValue)
	if err == nil {
		return nil
	}
	if err != nil && err != NothingToCheckErr {
		return err
	}

	err = pointerTypeRequirementForNumbers(fieldName, fieldValue)
	if err == nil {
		return nil
	}
	if err != nil && err != NothingToCheckErr {
		return err
	}

	return NothingToCheckErr
}

func pointerTypeRequirementForStrings(fieldName string, fieldValue interface{}) error {
	s, ok := fieldValue.(*string)
	if ok {
		if s == nil {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		} else if *s == "" {
			return fmt.Errorf(ErrorShouldNotBeEmpty, fieldName)
		}

		return nil
	}

	return NothingToCheckErr
}

func pointerTypeRequirementForNumbers(fieldName string, fieldValue interface{}) error {
	switch fieldValue.(type) {
	case *uint, *uint8, *uint16, *uint32, *uint64,
		*int, *int8, *int16, *int32, *int64,
		*float32, *float64:
		if fieldValue == nil {
			return fmt.Errorf(ErrorRequiredField, fieldName)
		}

		return nil
	}

	return NothingToCheckErr
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
