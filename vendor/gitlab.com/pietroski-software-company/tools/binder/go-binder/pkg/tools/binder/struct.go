package go_binder

import (
	"fmt"

	go_serializer "gitlab.com/pietroski-software-company/tools/serializer/go-serializer/pkg/tools/serializer"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"
)

func NewStructBinder(
	serializer go_serializer.Serializer,
	validator go_validator.Validator,
) Binder {
	return &structBinder{
		serializer: serializer,
		validator:  validator,
	}
}

func (b *structBinder) ShouldBind(payload interface{}, target interface{}) error {
	err := b.serializer.DataRebind(payload, target)
	if err != nil {
		return fmt.Errorf("error rebinding data; err: %v", err)
	}

	if err = b.validator.Validate(target); err != nil {
		return fmt.Errorf("error validating request payload; err: %v", err)
	}

	return nil
}
