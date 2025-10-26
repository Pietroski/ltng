package saga

type (
	// ListOperator is as the list operator class.
	ListOperator struct {
		operations []*Operation
	}

	// ChainOperator is as the linked-list operator class.
	ChainOperator struct {
		operation *Operation
	}

	// Operation holds the operation state control.
	Operation struct {
		Action   *Action
		Rollback *Rollback
	}

	// Action holds the action state control.
	Action struct {
		Name        string
		Do          func() error
		RetrialOpts *RetrialOpts

		Next *Operation
	}

	// Rollback holds the rollback state control action.
	Rollback struct {
		Name        string
		Do          func() error
		RetrialOpts *RetrialOpts

		Next *Operation
	}

	// RetrialOpts sets the retrial options.
	RetrialOpts struct {
		RetrialOnErr bool
		RetrialCount int
	}
)

var (
	// DefaultRetrialOps sets default values for retrial operations
	DefaultRetrialOps = &RetrialOpts{
		RetrialOnErr: true,
		RetrialCount: 2,
	}
)

const (
	failedToRetryRollbackMsg = "failed on rollback operation: %v"
	nothingToRetryMsg        = "no retry operation allowed"
	noOperationErrMsg        = "no operation found"

	retryCountExceededMsg = "retry count exceeded: %d"

	nilOpActionMsg      = "operation action is nil"
	nilOpRollbackActMsg = "operation rollback action is nil"
)
