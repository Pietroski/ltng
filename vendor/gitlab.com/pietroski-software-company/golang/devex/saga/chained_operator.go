package saga

import (
	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

// NewChainOperator returns a pointer to a new ChainOperator
func NewChainOperator(op *Operation) *ChainOperator {
	o := &ChainOperator{
		operation: op,
	}

	return o
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
func (o *ChainOperator) Operate() (err error) {
	if o.operation == nil || o.operation.Action == nil {
		return errorsx.New(noOperationErrMsg)
	}

	return o.action(o.operation)
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
func (o *ChainOperator) action(op *Operation) error {
	if op == nil || op.Action == nil || op.Action.Do == nil {
		//return errorsx.New(nilOpActionMsg)
		return nil
	}

	err := op.Action.Do()
	if err != nil {
		if op.Action.RetrialOpts.RetrialOnErr {
			err = coordinateRetry(op.Action.RetrialOpts, op.Action.Do)
			if err != nil { // if it fails to retry, it rolls back.
				rollbackErr := o.rollback(op)
				if rollbackErr != nil {
					return errorsx.Wrapf(err, failedToRetryRollbackMsg, rollbackErr)
				}

				return err
			}

			return o.action(op.Action.Next)
		}

		rollbackErr := o.rollback(op)
		if rollbackErr != nil {
			return errorsx.Wrapf(err, failedToRetryRollbackMsg, rollbackErr)
		}

		return err
	}

	if op.Action.Next == nil {
		return nil
	}

	return o.action(op.Action.Next)
}

func (o *ChainOperator) rollback(op *Operation) error {
	if op == nil || op.Rollback == nil || op.Rollback.Do == nil {
		// return errorsx.New(nilOpRollbackActMsg)
		return nil
	}

	err := op.Rollback.Do()
	if err != nil {
		if op.Rollback.RetrialOpts.RetrialOnErr {
			err = coordinateRetry(op.Rollback.RetrialOpts, op.Rollback.Do)
			if err != nil {
				// log the failure

				if op.Rollback.Next == nil {
					return nil
				}

				return o.rollback(op.Rollback.Next)
			}
		}

		if op.Rollback.Next == nil {
			return nil
		}

		return o.rollback(op.Rollback.Next)
	}

	if op.Rollback.Next == nil {
		return nil
	}

	return o.rollback(op.Rollback.Next)
}
