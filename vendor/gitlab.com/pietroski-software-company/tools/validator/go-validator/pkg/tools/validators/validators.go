package go_validator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type (
	Validator interface {
		Validate(obj interface{}) (err error)
		Use(customValidators map[string]CustomValidator) Validator
	}

	validation struct {
		obj         interface{}
		fieldNumber int
		fieldNames
		fieldTags
		fieldValues
		customValidators map[string]CustomValidator
	}

	pointerValidation struct {
		*validation
	}

	fieldTags       map[string]string
	fieldValues     map[string]interface{}
	fieldNames      []string
	CustomValidator func(
		fieldName string,
		fieldValue interface{},
	) error
)

func NewStructValidator() Validator {
	return &validation{}
}

func (v *validation) Validate(obj interface{}) (err error) {
	if reflect.TypeOf(obj).Kind() == reflect.Pointer {
		return v.pointerValidationFlow(obj)
	}

	return v.validationFlow(obj)
}

func (v *validation) validationFlow(obj interface{}) (err error) {
	v = v.newStructValidator(obj)

	err = v.extractFieldNames()
	err = v.extractStructFieldTags()
	err = v.extractStructFieldValues()

	return v.validate()
}

func (v *validation) newStructValidator(obj interface{}) *validation {
	objValueLen := v.extractStructFieldCount(obj) // reflect.ValueOf(obj).NumField()

	return &validation{
		obj:              obj,
		fieldNumber:      objValueLen,
		fieldNames:       make(fieldNames, objValueLen),
		fieldTags:        make(fieldTags, objValueLen),
		fieldValues:      make(fieldValues, objValueLen),
		customValidators: v.customValidators,
	}
}

func (v *validation) extractStructFieldCount(obj interface{}) int {
	if reflect.TypeOf(obj).Kind() == reflect.Pointer {
		return reflect.ValueOf(obj).Elem().NumField()
	}

	return reflect.ValueOf(obj).NumField()
}

func (v *validation) Use(customValidators map[string]CustomValidator) Validator {
	customValidatorsLen := len(customValidators)
	v.customValidators = make(map[string]CustomValidator, customValidatorsLen)
	for customValidatorKey, customValidatorVal := range customValidators {
		v.customValidators[customValidatorKey] = customValidatorVal
	}

	return v
}

func (v *validation) extractFieldNames() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractFieldNames recovered error %v", r)
		}
	}()

	typeOfObj := reflect.TypeOf(v.obj)
	for idx := 0; idx < v.fieldNumber; idx++ {
		v.fieldNames[idx] = typeOfObj.Field(idx).Name
	}

	return
}

func (v *validation) extractStructFieldTags() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractStructFieldTags recovered error %v", r)
		}
	}()

	typeOfObj := reflect.TypeOf(v.obj)
	for _, fieldName := range v.fieldNames {
		fieldValue, validFieldName := typeOfObj.FieldByName(fieldName)
		if !validFieldName {
			continue
		}
		if structTag, lookupOk := fieldValue.Tag.Lookup("validation"); lookupOk {
			v.fieldTags[fieldName] = structTag
		}
	}

	// TODO: remove it!!
	//printIndented(v.fieldTags)

	return nil
}

func (v *validation) extractStructFieldValues() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractStructFieldValues recovered error %v", r)
		}
	}()

	objValue := reflect.ValueOf(v.obj)
	for _, fieldName := range v.fieldNames {
		v.fieldValues[fieldName] = objValue.FieldByName(fieldName).Interface()
	}

	// TODO: remove it!!
	//printIndented(v.fieldValues)

	return nil
}

func (v *validation) validate() (err error) {
	for fieldName, fieldValue := range v.fieldValues {
		caseTypesMap := splitStrIntoStrMap(v.fieldTags[fieldName])
		if err = v.validationMapIterator(fieldName, fieldValue, caseTypesMap); err != nil {
			return err
		}
	}

	return nil
}

func (v *validation) validationMapIterator(
	fieldName string,
	fieldValue interface{},
	caseTypesMap map[string]string,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("validationMapIterator recovered error %v", r)
		}
	}() // recovery

	{ // mandatory validator
		if _, required := caseTypesMap["required"]; required {
			if err = checkRequirement(fieldName, fieldValue); err != nil {
				return
			}
		}
	}

	{ // length validators
		if minValue, hasMin := caseTypesMap["min"]; hasMin {
			if err = minLength(fieldName, fieldValue, minValue); err != nil {
				return
			}
		}

		if maxValue, hasMax := caseTypesMap["max"]; hasMax {
			if err = maxLength(fieldName, fieldValue, maxValue); err != nil {
				return
			}
		}

		if length, hasLength := caseTypesMap["len"]; hasLength {
			if err = eqLength(fieldName, fieldValue, length); err != nil {
				return
			}
		}
	}

	{ // type validators
		typeValName, isType := caseTypesMap["type"]
		if isType {
			if typeValName == "" {
				err = fmt.Errorf("type validator does not exist: type=%v", typeValName)
				return
			}

			if typeValName == emailType {
				if err = checkEmail(fieldName, fieldValue); err != nil {
					return
				}
			}

			if typeValName == passwordType {
				if err = checkPassword(fieldName, fieldValue); err != nil {
					return
				}
			}

			if typeValName == timeType {
				if err = checkTime(fieldName, fieldValue, caseTypesMap); err != nil {
					return
				}
			}
		}
	}

	{ // custom validators
		customValName, isCustom := caseTypesMap["custom"]
		if !isCustom {
			return
		}
		if customValName == "" {
			err = fmt.Errorf("custom validator does not exist: custom=%v", customValName)
			return
		}

		if v.customValidators == nil || len(v.customValidators) <= 0 {
			err = fmt.Errorf("custom validator exists but no validators were given")
			return
		}

		customVal, isThereCustomVal := v.customValidators[customValName]
		if !isThereCustomVal {
			err = fmt.Errorf("does not exist a validator with the given name: custom=%v", customValName)
			return
		}

		return customVal(fieldName, fieldValue)
	}
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

func printIndented(obj interface{}) (err error) {
	bfv, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println("Field values: ", string(bfv))

	return
}
