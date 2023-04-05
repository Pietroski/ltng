package go_validator

import (
	"fmt"
	"reflect"
	"strconv"
)

const (
	WrongMinFormatType      = "error: %v field has wrong min tag type format: %w"
	WrongMaxFormatType      = "error: %v field has wrong max tag type format: %w"
	WrongEqLengthFormatType = "error: %v field has wrong len tag type format: %w"

	ErrorSmallerThan = "error: %v has field value smaller than minimum tag validation value"
	ErrorGreaterThan = "error: %v has field value greater than maximum tag validation value"
	ErrorNotEqualTo  = "error: %v has field value not equal to tag validation value"
)

func minLength(fieldName string, fieldValue interface{}, strMinLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strMinLength, 64)
		if err != nil {
			return fmt.Errorf(WrongMinFormatType, fieldName, err)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv < floatVal {
			return fmt.Errorf(ErrorSmallerThan, fieldName)
		}

		return nil
	}

	minValue, err := strconv.Atoi(strMinLength) // strconv.ParseInt(strMinLength, 10, 64) //
	if err != nil {
		return fmt.Errorf(WrongMinFormatType, fieldName, err)
	}

	switch ft {
	case
		reflect.String,
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if fv.Len() < minValue {
			return fmt.Errorf(ErrorSmallerThan, fieldName)
		}
	case
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		fv := reflect.ValueOf(fieldValue).Int()
		if fv < int64(minValue) {
			return fmt.Errorf(ErrorSmallerThan, fieldName)
		}
	}

	return nil
}

func maxLength(fieldName string, fieldValue interface{}, strMinLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strMinLength, 64)
		if err != nil {
			return fmt.Errorf(WrongMinFormatType, fieldName, err)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv > floatVal {
			return fmt.Errorf(ErrorSmallerThan, fieldName)
		}

		return nil
	}

	maxValue, err := strconv.Atoi(strMinLength)
	if err != nil {
		return fmt.Errorf(WrongMaxFormatType, fieldName, err)
	}

	switch ft {
	case
		reflect.String,
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if fv.Len() > maxValue {
			return fmt.Errorf(ErrorGreaterThan, fieldName)
		}
	case
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		fv := reflect.ValueOf(fieldValue).Int()
		if fv > int64(maxValue) {
			return fmt.Errorf(ErrorGreaterThan, fieldName)
		}
	}

	return nil
}

func eqLength(fieldName string, fieldValue interface{}, strLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strLength, 64)
		if err != nil {
			return fmt.Errorf(WrongEqLengthFormatType, fieldName, err)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv != floatVal {
			return fmt.Errorf(ErrorNotEqualTo, fieldName)
		}

		return nil
	}

	lengthValue, err := strconv.Atoi(strLength)
	if err != nil {
		return fmt.Errorf(WrongEqLengthFormatType, fieldName, err)
	}

	switch ft {
	case
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		fv := reflect.ValueOf(fieldValue).Int()
		if fv != int64(lengthValue) {
			return fmt.Errorf(ErrorNotEqualTo, fieldName)
		}
	case
		reflect.String,
		reflect.Chan,
		reflect.Map,
		reflect.Array,
		reflect.Slice:
		fv := reflect.ValueOf(fieldValue).Len()
		if fv != lengthValue {
			return fmt.Errorf(ErrorNotEqualTo, fieldName)
		}
	}

	return nil
}
