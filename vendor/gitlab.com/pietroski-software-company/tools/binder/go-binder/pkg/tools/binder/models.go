package go_binder

import (
	serializer_models "gitlab.com/pietroski-software-company/devex/golang/serializer/models"
	go_validator "gitlab.com/pietroski-software-company/tools/validator/go-validator/pkg/tools/validators"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ../../../fakes/fake_binder.go . Binder
//go:generate mockgen -package mocks -destination ../../../mocks/mocked_binder.go . Binder

type (
	Binder interface {
		ShouldBind(payload interface{}, target interface{}) error
	}

	structBinder struct {
		serializer serializer_models.Serializer
		validator  go_validator.Validator
	}
)
