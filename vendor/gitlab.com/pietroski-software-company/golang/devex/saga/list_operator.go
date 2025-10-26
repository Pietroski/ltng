package saga

import (
	"fmt"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

// NewListOperator returns a pointer to a new ListOperator
func NewListOperator(ops ...*Operation) *ListOperator {
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
func (o *ListOperator) Operate() (err error) {
	if len(o.operations) == 0 {
		return fmt.Errorf(noOperationErrMsg)
	}

	return o.action(o.operations)
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
func (o *ListOperator) action(ops []*Operation) error {
	cursor := 0
	limit := len(ops)
	for ; cursor < limit; cursor++ {
		op := ops[cursor]
		if op.Action == nil || op.Action.Do == nil {
			continue
		}

		err := op.Action.Do()
		if err != nil {
			err = coordinateRetry(op.Action.RetrialOpts, op.Action.Do)
			if err != nil { // if it fails to retry, it rolls back.
				rollbackErr := o.rollback(ops, cursor)
				if rollbackErr != nil {
					return errorsx.Wrapf(err, failedToRetryRollbackMsg, rollbackErr)
				}

				return err
			}
		}
	}

	return nil
}

// rollback does control the rollback flow.
// it receives a cursor to start going back the operation list in order to
// revert all previously done actions.
func (o *ListOperator) rollback(ops []*Operation, cursor int) error {
	var err error
	for ; cursor >= 0; cursor-- {
		op := ops[cursor]
		if op.Rollback == nil || op.Rollback.Do == nil {
			continue
		}

		err = op.Rollback.Do()
		if err != nil {
			err = coordinateRetry(op.Action.RetrialOpts, op.Rollback.Do)
			if err != nil {
				err = errorsx.Wrapf(err, "rollback action name: %#v", op.Rollback)
			}
		}
	}
	if err != nil {
		return errorsx.Wrap(err, failedToRetryRollbackMsg)
	}

	return nil
}
