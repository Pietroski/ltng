package slogx

import (
	"log/slog"

	"gitlab.com/pietroski-software-company/golang/devex/errorsx"
)

// fmtErr returns a slog.Value with keys `msg` and `trace`. If the error
// does not implement interface { StackTrace() errors.StackTrace }, the `trace`
// key is omitted.
func fmtErr(err error) slog.Value {
	var groupValues []slog.Attr
	groupValues = append(groupValues, slog.String("msg", err.Error()))

	if errorsx.ToError(err) != nil {
		groupValues = append(groupValues,
			slog.Any("trace", errorsx.ToError(err).StackTrace()),
		)

		return slog.GroupValue(groupValues...)
	}

	frames := stacktrace()
	if frames != nil {
		groupValues = append(groupValues,
			slog.Any("trace", frames),
		)
	}

	return slog.GroupValue(groupValues...)
}
