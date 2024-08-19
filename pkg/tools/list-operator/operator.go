package list_operator

import "fmt"

type (
	// ListOperator is as the list operator class.
	ListOperator struct {
		operations []*Operation
	}

	// Operation holds the operation state control.
	Operation struct {
		Action   *Action
		Rollback *RollbackAction
	}

	// Action holds the action state control.
	Action struct {
		Act         func() error
		RetrialOpts *RetrialOpts
	}

	// RollbackAction holds the rollback state control action.
	RollbackAction struct {
		RollbackAct func() error
		RetrialOpts *RetrialOpts
	}

	// RetrialOpts sets the retrial options.
	RetrialOpts struct {
		RetrialOnErr bool
		RetrialCount int
	}

	// Error holds the error state of the list.
	// it is used to wrap the error history of faulty operations.
	Error struct {
		Type       ErrType
		ErrHistory error
		Err        error
	}
)

type (
	// ErrType determines the error state type
	// whether it is nil, retriable or non-retriable
	ErrType int
)

const (
	NilErr ErrType = iota
	RetriableErr
	UnretriableErr
)

var (
	// DefaultRetrialOps sets default values for retrial operations
	DefaultRetrialOps = &RetrialOpts{
		RetrialOnErr: true,
		RetrialCount: 2,
	}
)

const (
	failedToRetryActionMsg   = "failed to coordinate retry action operation"
	failedToRetryRollbackMsg = "failed to coordinate retry rollback operation"
	failedToRetryMsg         = "failed to coordinate retry operation"
	operationErrMsg          = "error occurred during operation"
	previousErrMsg           = "previous error occurred during operation"
)

// New return a pointer to a new ListOperator
func New(ops ...*Operation) *ListOperator {
	op := &ListOperator{
		operations: ops,
	}

	return op
}

// Operate triggers the list operator flow coordinating actions and rollbacks, and
// it returns a wrapped error of all failed operations.
//
// action controls the main action flow.
// If something goes wrong, a retry coordinator is called.
// If there is no retry options set for that action or
// if all the retry attempts have failed, then
// the rollback action is called.
//
// rollback does control the rollback flow.
// it receives a cursor to start going back the operation list in order to
// revert all previously done actions.
func (op *ListOperator) Operate() (err error) {
	if len(op.operations) == 0 {
		return fmt.Errorf("no operation found") // TODO: or return nil
	}

	Err := &Error{Type: NilErr}
	Err = action(op.operations, Err)

	return Err.ErrHistory
}

// action controls the main action flow.
// If something goes wrong, a retry coordinator is called.
// If there is no retry options set for that action or
// if all the retry attempts have failed, then
// the rollback action is called.
//
// rollback does control the rollback flow.
// it receives a cursor to start going back the operation list in order to
// revert all previously done actions.
func action(ops []*Operation, Err *Error) *Error {
	cursor := 0
	limit := len(ops)
	for ; cursor < limit; cursor++ {
		op := ops[cursor]
		if op.Action == nil || op.Action.Act == nil {
			continue
		}

		Err.Err = op.Action.Act()
		if Err.Err != nil {
			Err = coordinateRetry(op.Action.RetrialOpts, op.Action.Act, Err)
			if Err.Err != nil { // if it fails to retry, it rolls back.
				return rollback(ops, cursor, Err)
			}
		}
	}

	return Err
}

// rollback does control the rollback flow.
// it receives a cursor to start going back the operation list in order to
// revert all previously done actions.
func rollback(ops []*Operation, cursor int, Err *Error) *Error {
	for ; cursor >= 0; cursor-- {
		op := ops[cursor]
		if op.Rollback == nil || op.Rollback.RollbackAct == nil {
			continue
		}

		Err.Err = op.Rollback.RollbackAct()
		if Err.Err != nil {
			Err = coordinateRetry(op.Action.RetrialOpts, op.Rollback.RollbackAct, Err)
		}
	}

	return Err
}

// coordinateRetry coordinates the retry action.
func coordinateRetry(
	retryOpts *RetrialOpts,
	fn func() error,
	Err *Error,
) *Error {
	if retryOpts == nil || !retryOpts.RetrialOnErr {
		Err.ErrHistory = fmt.Errorf("%v: %w", operationErrMsg, Err.Err)
		return Err
	}

	retry := doRetry(retryOpts.RetrialCount, fn, Err)
	for Err.Type != UnretriableErr {
		Err = retry()
	}

	if Err.Err != nil {
		if Err.ErrHistory == nil {
			Err.ErrHistory = fmt.Errorf("%v: %w", failedToRetryMsg, Err.Err)

			return Err
		}

		Err.ErrHistory = fmt.Errorf("%v: %w: %w", failedToRetryMsg, Err.Err,
			fmt.Errorf("%v: %w", previousErrMsg, Err.ErrHistory))
	}

	return Err
}

// doRetry takes care of the retry state and count.
func doRetry(
	count int,
	fn func() error,
	Err *Error,
) func() *Error {
	var counter int
	limit := count
	Err.Type = NilErr

	return func() *Error {
		if counter < limit {
			counter++

			Err.Err = fn()
			if Err.Err == nil {
				Err.Type = UnretriableErr
				return Err
			}

			Err.Type = RetriableErr
			return Err
		}

		Err.Type = UnretriableErr
		return Err
	}
}
