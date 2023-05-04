package custom_validators

import (
	"fmt"

	"github.com/google/uuid"
)

func UUIDValidator(
	fieldName string,
	fieldValue interface{},
) error {
	if value, ok := fieldValue.(uuid.UUID); ok {
		if value == [16]byte{} {
			return fmt.Errorf("empty uuid at %v", fieldName)
		}

		return nil
	}

	return fmt.Errorf("invalid uuid")
}

func StringUUIDValidator(
	fieldName string,
	fieldValue interface{},
) error {
	if value, ok := fieldValue.(string); ok {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("invalid uuid parse at field: %v; err: %v", fieldName, err)
		}

		return nil
	}

	return fmt.Errorf("invalid uuid")
}
