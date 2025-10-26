package saga

import (
	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

// coordinateRetry coordinates the retry action.
func coordinateRetry(
	retryOpts *RetrialOpts,
	fn func() error,
) error {
	if retryOpts == nil || !retryOpts.RetrialOnErr {
		return errorsx.New(nothingToRetryMsg)
	}

	retry := doRetry(retryOpts.RetrialCount, fn)
	err := retry()
	for err != nil {
		if errorsx.IsNonRetriable(err) {
			return err
		}

		err = retry()
	}

	return nil
}

// doRetry takes care of the retry state and count.
func doRetry(
	count int,
	fn func() error,
) func() error {
	var counter int
	limit := count

	var err error
	return func() error {
		counter++
		if counter <= limit {
			err = fn()
			return err
		}

		if err != nil {
			return errorsx.Wrapf(err, retryCountExceededMsg, count).(*errorsx.Error).WithNonRetriable()
		}

		return nil
	}
}
