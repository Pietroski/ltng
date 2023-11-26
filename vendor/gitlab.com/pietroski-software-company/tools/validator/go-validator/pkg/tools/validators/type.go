package go_validator

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	strings_parser "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/parsers/strings"
	custom_validators "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators/custom"
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

var (
	rxEmail = regexp.MustCompile(emailValRegStr)
)

func checkEmail(fieldName string, fieldValue interface{}) (err error) {
	if err = minLength(fieldName, fieldValue, fmt.Sprintf("%d", minEmailLength)); err != nil {
		return err
	}
	if err = maxLength(fieldName, fieldValue, fmt.Sprintf("%d", maxEmailLength)); err != nil {
		return err
	}

	emailValue := reflect.ValueOf(fieldValue).String()
	if !rxEmail.MatchString(emailValue) {
		return fmt.Errorf(ErrorInvalidFieldFormat, fieldName, "invalid email format")
	}

	return nil
}

func checkPassword(fieldName string, fieldValue interface{}) (err error) {
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
		return fmt.Errorf(ErrorInvalidFieldFormat, fieldName, "invalid password format")
	}

	return nil
}

func checkTime(
	fieldName string, fieldValue interface{},
	caseTypesMap map[string]string,
) (err error) {
	format, isFormat := caseTypesMap["format"]
	timeValue, ok := fieldValue.(time.Time)
	if !ok {
		return fmt.Errorf("wrong matched type: field: %v - value: %v", fieldName, fieldValue)
	}

	var timeComparator time.Time
	afterTimeStr, ok := caseTypesMap["after"]
	if ok {
		timeComparator, err = strings_parser.TimeParser(afterTimeStr, format, isFormat)
		if err != nil {
			return fmt.Errorf(ErrorTimeParser, err)
		}
		if timeValue.After(timeComparator) {
			return
		}

		return fmt.Errorf(ErrorAfterTime, timeValue, timeComparator)
	}

	beforeTimeStr, ok := caseTypesMap["before"]
	if ok {
		timeComparator, err = strings_parser.TimeParser(beforeTimeStr, format, isFormat)
		if err != nil {
			return fmt.Errorf(ErrorTimeParser, err)
		}
		if timeValue.Before(timeComparator) {
			return
		}

		return fmt.Errorf(ErrorBeforeTime, timeValue, timeComparator)
	}

	return
}

func checkUUID(
	fieldName string, fieldValue interface{},
) (err error) {
	return custom_validators.UUIDValidator(fieldName, fieldValue)
}
