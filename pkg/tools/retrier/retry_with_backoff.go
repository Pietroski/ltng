package retrier

import "time"

type RetryWithBackOff struct {
	MaxRetryCount          int
	InitialBackoffDuration time.Duration
	BackoffIncreaseFactor  time.Duration
}

// RetryWithBackoff retries calling a callback function for n amount of time with exponential backoff.
func (rwb *RetryWithBackOff) RetryWithBackoff(fn func() error) error {
	count := 1
	wait := rwb.InitialBackoffDuration

	err := fn()
	for err != nil && count < rwb.MaxRetryCount {
		time.Sleep(wait)
		count++
		err = fn()
		if err != nil {
			wait *= rwb.BackoffIncreaseFactor
		}
	}

	return err
}
