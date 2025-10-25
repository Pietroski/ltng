package validator

import (
	"reflect"
	"strconv"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

const (
	WrongMinFormatType      = "error: %v field has wrong min tag type format"
	WrongMaxFormatType      = "error: %v field has wrong max tag type format"
	WrongEqLengthFormatType = "error: %v field has wrong len tag type format"

	ErrorSmallerThan = "error: %v has field value smaller than minimum tag validation value"
	ErrorGreaterThan = "error: %v has field value greater than maximum tag validation value"
	ErrorNotEqualTo  = "error: %v has field value not equal to tag validation value"
)

func minLenValidator(
	fieldName string, fieldValue any, parameter string,
	_ map[string]string,
) error {
	return minLength(fieldName, fieldValue, parameter)
}

func minLength(fieldName string, fieldValue any, strMinLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	if ft == reflect.Pointer {
		fieldValue = reflect.ValueOf(fieldValue).Elem().Interface()
		ft = reflect.TypeOf(fieldValue).Kind()
	}

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strMinLength, 64)
		if err != nil {
			return errorsx.Wrapf(err, WrongMinFormatType, fieldName)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv < floatVal {
			return errorsx.Errorf(ErrorSmallerThan, fieldName)
		}

		return nil
	}

	minValue, err := strconv.Atoi(strMinLength) // strconv.ParseInt(strMinLength, 10, 64) //
	if err != nil {
		return errorsx.Wrapf(err, WrongMinFormatType, fieldName)
	}

	switch ft {
	case
		reflect.String,
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if fv.Len() < minValue {
			return errorsx.Errorf(ErrorSmallerThan, fieldName)
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
			return errorsx.Errorf(ErrorSmallerThan, fieldName)
		}
	}

	return nil
}

func maxLenValidator(
	fieldName string, fieldValue any, parameter string,
) func() error {
	return func() error {
		return maxLength(fieldName, fieldValue, parameter)
	}
}

func maxLength(fieldName string, fieldValue any, strMinLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	if ft == reflect.Pointer {
		fieldValue = reflect.ValueOf(fieldValue).Elem().Interface()
		ft = reflect.TypeOf(fieldValue).Kind()
	}

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strMinLength, 64)
		if err != nil {
			return errorsx.Wrapf(err, WrongMinFormatType, fieldName)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv > floatVal {
			return errorsx.Errorf(ErrorSmallerThan, fieldName)
		}

		return nil
	}

	maxValue, err := strconv.Atoi(strMinLength)
	if err != nil {
		return errorsx.Wrapf(err, WrongMaxFormatType, fieldName)
	}

	switch ft {
	case
		reflect.String,
		reflect.Slice,
		reflect.Array:
		fv := reflect.ValueOf(fieldValue)
		if fv.Len() > maxValue {
			return errorsx.Errorf(ErrorGreaterThan, fieldName)
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
			return errorsx.Errorf(ErrorGreaterThan, fieldName)
		}
	}

	return nil
}

func eqLenValidator(
	fieldName string, fieldValue any, parameter string,
) func() error {
	return func() error {
		return eqLength(fieldName, fieldValue, parameter)
	}
}

func eqLength(fieldName string, fieldValue any, strLength string) error {
	ft := reflect.TypeOf(fieldValue).Kind()

	if ft == reflect.Pointer {
		fieldValue = reflect.ValueOf(fieldValue).Elem().Interface()
		ft = reflect.TypeOf(fieldValue).Kind()
	}

	switch ft {
	case
		reflect.Float32,
		reflect.Float64:
		floatVal, err := strconv.ParseFloat(strLength, 64)
		if err != nil {
			return errorsx.Wrapf(err, WrongEqLengthFormatType, fieldName)
		}
		fv := reflect.ValueOf(fieldValue).Float()
		if fv != floatVal {
			return errorsx.Errorf(ErrorNotEqualTo, fieldName)
		}

		return nil
	}

	lengthValue, err := strconv.Atoi(strLength)
	if err != nil {
		return errorsx.Wrapf(err, WrongEqLengthFormatType, fieldName)
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
			return errorsx.Errorf(ErrorNotEqualTo, fieldName)
		}
	case
		reflect.String,
		reflect.Chan,
		reflect.Map,
		reflect.Array,
		reflect.Slice:
		fv := reflect.ValueOf(fieldValue).Len()
		if fv != lengthValue {
			return errorsx.Errorf(ErrorNotEqualTo, fieldName)
		}
	}

	return nil
}
