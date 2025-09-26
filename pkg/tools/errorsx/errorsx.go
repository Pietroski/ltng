package errorsx

import "errors"

var (
	ErrRetryable   = errors.New("retryable error")
	ErrUnRetryable = errors.New("unretryable error")

	ErrUnknown = errors.New("unknown error")
)
