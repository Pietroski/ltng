package slogx

import "log/slog"

const (
	LevelTrace = slog.Level(-8)
	LevelTest  = slog.Level(-12)
	LevelPanic = slog.Level(12)
	LevelFatal = slog.Level(16)
)

// LevelNames is the list of supported extra log levels
var LevelNames = map[slog.Leveler]string{
	LevelTrace: "TRACE",
	LevelTest:  "TEST",
	LevelPanic: "PANIC",
	LevelFatal: "FATAL",
}
