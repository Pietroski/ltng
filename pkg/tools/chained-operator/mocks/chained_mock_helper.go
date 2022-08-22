package chained_mock

type (
	Callers interface {
		ActionFunc1() error
		ActionFunc2() error
		ActionFunc3() error
		RollbackActionFunc1() error
		RollbackActionFunc2() error
	}
)
