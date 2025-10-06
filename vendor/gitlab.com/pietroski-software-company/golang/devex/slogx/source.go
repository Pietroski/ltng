package slogx

import "log/slog"

// fmtSrc returns a slog.Value with `trace` key.
// fmtSrc call only occurs if slog.HandlerOptions.AddSource is set to true
func fmtSrc(src any) slog.Value {
	var groupValues []slog.Attr
	groupValues = append(groupValues, slog.Any("msg", src))

	frames := stacktrace()
	if frames != nil {
		groupValues = append(groupValues,
			slog.Any("trace", frames),
		)
	}

	return slog.GroupValue(groupValues...)
}
