package validator

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

const (
	emailType    = "email"
	passwordType = "password"
	timeType     = "time"
	uuidType     = "uuid"

	ErrorInvalidFieldFormat = "error: %v field has invalid format: %v"
	ErrorAfterTime          = "time %v should be after %v"
	ErrorBeforeTime         = "time %v should be before %v"
	ErrorTimeParser         = "failed to parse given time: %v"
)

const (
	emailValRegStr = "^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
	minEmailLength = 3
	maxEmailLength = 256

	minPassLength = 8
	maxPassLength = 256
)

var rxEmail = regexp.MustCompile(emailValRegStr)

func checkEmail(fieldName string, fieldValue any) (err error) {
	if err = minLength(fieldName, fieldValue, fmt.Sprintf("%d", minEmailLength)); err != nil {
		return err
	}
	if err = maxLength(fieldName, fieldValue, fmt.Sprintf("%d", maxEmailLength)); err != nil {
		return err
	}

	emailValue := reflect.ValueOf(fieldValue).String()
	if !rxEmail.MatchString(emailValue) {
		return errorsx.Errorf(ErrorInvalidFieldFormat, fieldName, "invalid email format")
	}

	return nil
}

func checkPassword(fieldName string, fieldValue any) (err error) {
	passwordValue := reflect.ValueOf(fieldValue).String()
	trimmedPassword := strings.TrimSpace(passwordValue)

	if err = minLength(fieldName, trimmedPassword, fmt.Sprintf("%d", minPassLength)); err != nil {
		return err
	}
	if err = maxLength(fieldName, trimmedPassword, fmt.Sprintf("%d", maxPassLength)); err != nil {
		return err
	}

	hasUppercase, hasLowercase, hasDigit, hasSpecialChar := false, false, false, false
	for _, r := range trimmedPassword {
		switch {
		case unicode.IsUpper(r):
			hasUppercase = true
		case unicode.IsLower(r):
			hasLowercase = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			hasSpecialChar = true
		}

		if hasUppercase && hasLowercase && hasDigit && hasSpecialChar {
			break
		}
	}
	if !(hasUppercase && hasLowercase && hasDigit && hasSpecialChar) {
		return errorsx.Errorf(ErrorInvalidFieldFormat, fieldName, "invalid password format")
	}

	return nil
}

func checkTime(fieldName string, fieldValue any, _ string) error {
	timeValue, ok := fieldValue.(time.Time)
	if !ok {
		return errorsx.Errorf(
			"wrong matched type. Field value is not of type time: field: %v - value: %v",
			fieldName, fieldValue)
	}

	if timeValue.IsZero() {
		return errorsx.Errorf("time value is zero but it cannot be zero: field: %v", fieldName)
	}

	return nil
}

const (
	InvalidUUIDErrMsg = "invalid uuid"
)

func uuidValidator(
	fieldName string,
	fieldValue any,
	_ string,
) error {
	if value, ok := fieldValue.(uuid.UUID); ok {
		if value == [16]byte{} {
			return errorsx.Errorf("empty uuid at %v", fieldName)
		}

		return nil
	}

	return errorsx.New(InvalidUUIDErrMsg)
}

func stringUUIDValidator(
	fieldName string,
	fieldValue any,
	_ string,
) error {
	if value, ok := fieldValue.(string); ok {
		if _, err := uuid.Parse(value); err != nil {
			return errorsx.Errorf("invalid uuid parse at field: %v; err: %v", fieldName, err)
		}

		return nil
	}

	return errorsx.New(InvalidUUIDErrMsg)
}

func (v *validation) checkCustom(
	fieldName string, fieldValue any,
	caseTypesMap map[string]string,
) error {
	customValName, isCustom := caseTypesMap["custom"]
	if !isCustom {
		return nil
	}

	if customValName == "" {
		return errorsx.New("custom validator is empty but it is required")
	}

	if v.customValidators == nil || len(v.customValidators) <= 0 {
		return errorsx.New("custom validators were not setup")
	}

	customVal, isThereCustomVal := v.customValidators[customValName]
	if !isThereCustomVal {
		return errorsx.New(fmt.Sprintf(
			"does not exist a validator with the given name: custom=%v",
			customValName))
	}

	return customVal(fieldName, fieldValue)
}

func checkType(fieldName string, fieldValue any, parameter string) error {
	switch parameter {
	case emailType:
		return checkEmail(fieldName, fieldValue)
	case passwordType:
		return checkPassword(fieldName, fieldValue)
	case timeType:
		return checkTime(fieldName, fieldValue, parameter)
	case uuidType:
		return uuidValidator(fieldName, fieldValue, parameter)
	default:
		return errorsx.Errorf("unknown type: %v", parameter)
	}
}
