package retry_mechanism

// retrialOperator implements a simple retrial execution policy.
func retrialOperator(err error, fn func() error, maxRetries int) error {
	retryCount := 0
	for err != nil || retryCount < maxRetries {
		err = fn()

		retryCount++
	}

	return err
}
