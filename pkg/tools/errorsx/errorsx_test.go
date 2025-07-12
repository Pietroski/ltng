package errorsx

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	t.Run("TestErrors", func(t *testing.T) {
		testErr := errors.New("test error")
		err := fmt.Errorf("test error: %w: %w", testErr, ErrUnknown)

		assert.True(t, errors.Is(err, testErr))
		assert.True(t, errors.Is(err, ErrUnknown))
		assert.False(t, errors.Is(err, errors.New("test error")))
		assert.False(t, errors.Is(err, fmt.Errorf("test error")))
		assert.False(t, errors.Is(err, fmt.Errorf("test error: %w", ErrUnknown)))
	})
}
