package errorsx

import (
	"fmt"
	"runtime"
	"strings"
)

type (
	// StackTracer interface for errors that can provide stack traces
	StackTracer interface {
		StackTrace() string
	}

	Causer interface {
		Cause() error
	}

	// ErrType determines the error state type
	// whether it is nil, retriable or non-retriable
	ErrType int

	// Error holds the error state of the list.
	// it is used to wrap the error history of faulty operations.
	Error struct {
		errType    ErrType
		message    string
		stacktrace []uintptr

		next *Error
	}
)

const (
	UnknownErr ErrType = iota
	RetriableErr
	NonRetriableErr
)

// New instantiates a new Error.
func New(message string) *Error {
	return &Error{
		message:    message,
		stacktrace: callers(),
	}
}

// Errorf formats according to a format specifier and returns the string as an error.
func Errorf(format string, args ...any) *Error {
	return &Error{
		message:    fmt.Sprintf(format, args...),
		stacktrace: callers(),
	}
}

func From(err error) *Error {
	if err == nil {
		return nil
	}

	return &Error{
		message:    err.Error(),
		stacktrace: callers(),
	}
}

// Wrap wraps the error
//
// Also, to further know:
// The idea of appending or prepending stack traces depends on how you want to represent the causal chain of errors.
// In most cases, especially in Go's error handling philosophy (and following libraries like pkg/errors ),
// we prefer to:
// - capture the stack trace at the point where the error was first created, not where it is wrapped;
// - when wrapping an error:
//   - if it’s already a stack-traced error, reuse its existing stack (don't capture again);
//   - If it's a plain error, then capture the current stack trace.
//
// So, we do not append to the existing stack trace because:
// - the first error in the chain (the root cause) should have the most relevant stack trace.
// - wrapping layers add context but shouldn’t change or duplicate that information.
func Wrap(err error, message string) error {
	msg := fmt.Sprintf("%s: %v", message, err)

	if e, ok := err.(*Error); ok {
		return &Error{
			message:    msg,
			stacktrace: e.stacktrace,
			errType:    e.errType,
			next:       e,
		}
	}

	return New(msg)
}

// Wrapf wraps the error with a formatted message.
func Wrapf(err error, format string, args ...any) error {
	msg := fmt.Sprintf("%s: %v", fmt.Sprintf(format, args...), err)

	if e, ok := err.(*Error); ok {
		return &Error{
			message:    msg,
			stacktrace: e.stacktrace,
			errType:    e.errType,
			next:       e,
		}
	}

	return New(msg)
}

// Wrap wraps the Error into an Error
//
// Also, to further know:
// The idea of appending or prepending stack traces depends on how you want to represent the causal chain of errors.
// In most cases, especially in Go's error handling philosophy (and following libraries like pkg/errors ),
// we prefer to:
// - capture the stack trace at the point where the error was first created, not where it is wrapped;
// - when wrapping an error:
//   - if it’s already a stack-traced error, reuse its existing stack (don't capture again);
//   - If it's a plain error, then capture the current stack trace.
//
// So, we do not append to the existing stack trace because:
// - the first error in the chain (the root cause) should have the most relevant stack trace.
// - wrapping layers add context but shouldn’t change or duplicate that information.
func (e *Error) Wrap(err error, message string) *Error {
	return &Error{
		message:    fmt.Sprintf("%s: %v", message, err),
		stacktrace: e.stacktrace,
		errType:    e.errType,
		next:       e,
	}
}

// Wrapf wraps the Error with a formatted message into an Error.
func (e *Error) Wrapf(err error, format string, args ...any) *Error {
	return &Error{
		message:    fmt.Sprintf("%s: %v", fmt.Sprintf(format, args...), err),
		stacktrace: e.stacktrace,
		errType:    e.errType,
		next:       e,
	}
}

// Errorf formats according to a format specifier and returns the string as an error.
func (e *Error) Errorf(format string, args ...any) *Error {
	return &Error{
		message:    fmt.Sprintf(format, args...),
		stacktrace: e.stacktrace,
		errType:    e.errType,
		next:       e,
	}
}

// WithRetriable sets a retriable error type to the *Error instance.
func (e *Error) WithRetriable() *Error {
	e.errType = RetriableErr
	return e
}

// WithNonRetriable sets a non-retriable error type to the *Error instance.
func (e *Error) WithNonRetriable() *Error {
	e.errType = NonRetriableErr
	return e
}

// IsRetriable checks whether the errType is a retriable error type.
func (e *Error) IsRetriable() bool {
	return e.errType == RetriableErr
}

// IsNonRetriable checks whether the errType is a non-retriable error type.
func (e *Error) IsNonRetriable() bool {
	return e.errType == NonRetriableErr
}

// IsUnknown checks whether the errType is a unknown error type.
func (e *Error) IsUnknown() bool {
	return e.errType == UnknownErr
}

// As implements the errors.As interface.
func (e *Error) As(target any) bool {
	if t, ok := target.(**Error); ok {
		*t = e

		return true
	}

	return false
}

func Is(err error, target error) bool {
	e, ok := err.(*Error)
	if !ok {
		return false
	}

	return e.Is(target)
}

// Is implements the errors.Is interface.
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return e == target
	}

	t, ok := target.(*Error)
	if !ok {
		return false
	}

	for e != nil {
		//if e.message == t.message {
		//	return true
		//}
		if strings.Contains(e.message, t.message) {
			return true
		}

		e = e.next
	}

	return false
}

// IsRetriable checks whether the error is a retriable error type.
func IsRetriable(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.errType == RetriableErr
	}

	return false
}

// IsNonRetriable checks whether the error is a non-retriable error type.
func IsNonRetriable(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.errType == NonRetriableErr
	}

	return false
}

// IsUnknown checks whether the error is a unknown error type.
func IsUnknown(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.errType == UnknownErr
	}

	return false
}

// Unwrap implements the errors.Unwrap interface.
func Unwrap(err error) error {
	if e, ok := err.(*Error); ok {
		return e.next
	}

	return nil
}

// ToError type check and casts an error to *Error.
// if error is not of type *Error, it then does create an *Error
// from that error.
// if the given error is nil, it returns nil.
func ToError(err error) *Error {
	if err == nil {
		return nil
	}

	if e, ok := err.(*Error); ok {
		return e
	}

	// For non‑*Error values, return nil to indicate conversion failure.
	return nil
}

// Unwrap implements the errors.Unwrap interface.
func (e *Error) Unwrap() error {
	if e == nil || e.next == nil {
		return nil
	}

	return e.next
}

// Cause returns the root cause of the error.
func (e *Error) Cause() error {
	return e.Root()
}

// Root return the underlying root error.
// It has an internal safeguard to avoid infinite loops.
func (e *Error) Root() *Error {
	seen := map[*Error]struct{}{}

	current := e
	seen[current] = struct{}{}
	for current.next != nil {
		current = current.next
		if _, ok := seen[current]; ok {
			return current
		}

		seen[current] = struct{}{}
	}

	return current
}

// Error implements the error interface.
func (e *Error) Error() string {
	return e.message
}

// StackTrace returns the formatted stack trace.
func (e *Error) StackTrace() string {
	if e == nil || len(e.stacktrace) <= 0 {
		return ""
	}

	var buf strings.Builder

	frames := runtime.CallersFrames(e.stacktrace)
	for {
		frame, more := frames.Next()
		buf.WriteString(fmt.Sprintf("%s:%d:%s\n", frame.File, frame.Line, frame.Function))
		if !more {
			break
		}
	}

	return buf.String()
}

// caller captures the current stack trace.
func callers() []uintptr {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:]) // Skip New/Errorf and captureStackTrace

	return pcs[:n-2]
}

// Ensure Error implements the interfaces
var (
	_ StackTracer = (*Error)(nil)
	_ Causer      = (*Error)(nil)
)
