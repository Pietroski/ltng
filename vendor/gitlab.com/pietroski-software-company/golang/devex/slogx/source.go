package slogx

import "log/slog"

// fmtSrc returns a slog.Value with `trace` key.
// fmtSrc call only occurs if slog.HandlerOptions.AddSource is set to true
func fmtSrc() slog.Value {
	var groupValues []slog.Attr

	frames := stacktrace()
	if frames != nil {
		groupValues = append(groupValues,
			slog.Any("trace", frames),
		)
	}

	return slog.GroupValue(groupValues...)
}
