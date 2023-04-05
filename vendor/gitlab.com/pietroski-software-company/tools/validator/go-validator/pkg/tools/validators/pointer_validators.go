package go_validator

import (
	"fmt"
	"reflect"
)

func (v *validation) pointerValidationFlow(obj interface{}) (err error) {
	pv := v.newPrtStructValidator(obj)

	err = pv.extractFieldNames()
	err = pv.extractStructFieldTags()
	err = pv.extractStructFieldValues()

	return pv.validate()
}

func (v *validation) newPrtStructValidator(obj interface{}) *pointerValidation {
	return &pointerValidation{
		v.newStructValidator(obj),
	}
}

func (v *pointerValidation) extractFieldNames() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractFieldNames recovered error %v", r)
		}
	}()

	typeOfObj := reflect.TypeOf(v.obj).Elem()
	for idx := 0; idx < v.fieldNumber; idx++ {
		v.fieldNames[idx] = typeOfObj.Field(idx).Name
	}

	return
}

func (v *pointerValidation) extractStructFieldTags() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractStructFieldTags recovered error %v", r)
		}
	}()

	typeOfObj := reflect.TypeOf(v.obj).Elem()
	for _, fieldName := range v.fieldNames {
		fieldValue, validFieldName := typeOfObj.FieldByName(fieldName)
		if !validFieldName {
			continue
		}
		if structTag, lookupOk := fieldValue.Tag.Lookup("validation"); lookupOk {
			v.fieldTags[fieldName] = structTag
		}
	}

	return nil
}

func (v *pointerValidation) extractStructFieldValues() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extractStructFieldValues recovered error %v", r)
		}
	}()

	objValue := reflect.ValueOf(v.obj).Elem()
	for _, fieldName := range v.fieldNames {
		v.fieldValues[fieldName] = objValue.FieldByName(fieldName).Interface()
	}

	return nil
}
