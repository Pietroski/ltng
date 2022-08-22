package chainded_operator

import (
	"fmt"
)

type (
	ErrType int
)

const (
	NilErr ErrType = iota
	RetriableErr
	UnretriableErr
)

type (
	ChainOperator struct{}

	Ops struct {
		Action         *Action
		RollbackAction *RollbackAction
	}

	Action struct {
		Act         func() error
		RetrialOpts *RetrialOpts
		Next        *Ops
	}

	RollbackAction struct {
		RollbackAct func() error
		RetrialOpts *RetrialOpts
		Next        *Ops
	}

	RetrialOpts struct {
		RetrialOnErr bool
		RetrialCount int
	}

	Error struct {
		Type        ErrType
		ActionErr   error
		RollbackErr error
		Err         error
	}
)

var (
	DefaultRetrialOps = &RetrialOpts{
		RetrialOnErr: true,
		RetrialCount: 2,
	}
)

func NewChainOperator() *ChainOperator {
	return &ChainOperator{}
}

func (o *ChainOperator) Operate(ops *Ops) (err error) {
	if ops == nil {
		return nil
	}

	Err := &Error{Type: NilErr}
	Err = o.action(ops, Err)
	err = o.errorWrapper(Err)

	return
}

func (o *ChainOperator) action(ops *Ops, Err *Error) *Error {
	if ops == nil || ops.Action == nil || ops.Action.Act == nil {
		return Err
	}

	Err.Err = ops.Action.Act()
	if Err.Err != nil {
		if ops.Action.RetrialOpts.RetrialOnErr {
			retry := doRetry(
				ops.Action.RetrialOpts.RetrialCount, ops.Action.Act, Err)
			for Err.Type != UnretriableErr {
				Err = retry()
			}

			if Err.Err == nil {
				return o.action(ops.Action.Next, Err)
			}
		}

		o.wrapActionErr(Err, Err.Err)
		Err.Type = RetriableErr
		return o.rollback(ops, Err)
	}

	return o.action(ops.Action.Next, Err)
}

func (o *ChainOperator) rollback(ops *Ops, Err *Error) *Error {
	if ops == nil || ops.RollbackAction == nil || ops.RollbackAction.RollbackAct == nil {
		return Err
	}

	Err.Err = ops.RollbackAction.RollbackAct()
	if Err.Err != nil {
		if ops.RollbackAction.RetrialOpts.RetrialOnErr {
			retry := doRetry(
				ops.RollbackAction.RetrialOpts.RetrialCount, ops.RollbackAction.RollbackAct, Err)
			for Err.Type != UnretriableErr {
				Err = retry()
			}
		}

		o.wrapRollbackErr(Err, Err.Err)
	}

	return o.rollback(ops.RollbackAction.Next, Err)
}

func doRetry(
	count int,
	fn func() error,
	Err *Error,
) func() *Error {
	var counter int
	limit := count

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

func (o *ChainOperator) wrapActionErr(Err *Error, err error) {
	if Err.RollbackErr != nil || err != nil {
		Err.ActionErr = fmt.Errorf("%v -:- %v", Err.ActionErr, err)
	}
}

func (o *ChainOperator) wrapRollbackErr(Err *Error, err error) {
	if Err.RollbackErr != nil || err != nil {
		Err.RollbackErr = fmt.Errorf("%v -:- %v", Err.RollbackErr, err)
	}
}

func (o *ChainOperator) errorWrapper(Err *Error) (err error) {
	if Err.ActionErr != nil {
		err = fmt.Errorf("action-errors: %v", Err.ActionErr)
	}
	if Err.RollbackErr != nil {
		err = fmt.Errorf("%v\nrollback-errors: %v\n", err, Err.RollbackErr)
	}

	return
}
