package list_operator_mocks

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ./list_operator_fakes/fake_callers.go . Callers

type (
	Callers interface {
		ActionFunc1() error
		ActionFunc2() error
		ActionFunc3() error
		RollbackActionFunc1() error
		RollbackActionFunc2() error
	}

	PreCallers interface {
		ActionFunc1() func() error
		ActionFunc2() func() error
		ActionFunc3() func() error
		RollbackActionFunc1() func() error
		RollbackActionFunc2() func() error
	}
)
