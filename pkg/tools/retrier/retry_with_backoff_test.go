package retrier

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryWithBackoff(t *testing.T) {
	retrier := &RetryWithBackOff{
		MaxRetryCount:          3,
		InitialBackoffDuration: time.Millisecond * 250,
		BackoffIncreaseFactor:  time.Duration(1),
	}
	testErr := fmt.Errorf("test error")

	t.Run("happy-path", func(t *testing.T) {
		respChan := make(chan error, 5)
		respChan <- testErr
		respChan <- testErr
		respChan <- nil

		err := retrier.RetryWithBackoff(func() error {
			return <-respChan
		})
		require.NoError(t, err)
		t.Log(err)
	})

	t.Run("error after last retry", func(t *testing.T) {
		respChan := make(chan error, 5)
		respChan <- testErr
		respChan <- testErr
		respChan <- testErr

		err := retrier.RetryWithBackoff(func() error {
			return <-respChan
		})
		require.Error(t, err)
		t.Log(err)
	})
}
