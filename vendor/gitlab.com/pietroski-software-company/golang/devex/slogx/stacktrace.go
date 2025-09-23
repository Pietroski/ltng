package slogx

import (
	"math"
	"path/filepath"
	"runtime"
	"strings"
)

const stackTraceFrames = math.MaxUint8

type Frame struct {
	Function string `json:"function,omitempty"`
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
}

// StackTrace it provides the stacktrace frame list publicly.
//
// stacktrace parses the output from debug.Stack() into frames and filters out unwanted frames.
func StackTrace() []*Frame {
	return stacktrace()
}

// stacktrace parses the output from debug.Stack() into frames and filters out unwanted frames.
func stacktrace() []*Frame {
	// Ask runtime.Callers for up to 10 PCs, including runtime.Callers itself.
	pc := make([]uintptr, stackTraceFrames)
	n := runtime.Callers(0, pc)
	if n == 0 {
		// No PCs available. This can happen if the first argument to
		// runtime.Callers is large.
		//
		// Return now to avoid processing the zero Frame that would
		// otherwise be returned by frames.Next below.
		return nil
	}

	pc = pc[:n] // pass only valid pcs to runtime.CallersFrames

	frames := runtime.CallersFrames(pc)

	var frameList []*Frame
	for {
		frame, more := frames.Next()
		if !more { // checks if the item is valid right away. If it is not, it does not append the invalid frame.
			break
		}

		// Process this frame.
		//
		// it filters out the unwanted frames from debug.Stack() method call
		if patternSkip(frame.File) {
			continue
		}

		//{
		//	//fmt.Printf("%v - %v\n", frame, frame.Func)
		//	fmt.Printf("%s:%d %s\n",
		//		frame.File, frame.Line,
		//		filepath.Base(frame.Function))
		//}

		frameItem := Frame{
			Function: filepath.Base(frame.Function),
			File:     frame.File,
			Line:     frame.Line,
		}
		frameList = append(frameList, &frameItem)
	}

	return frameList
}

// patterns is the list of frame patterns which will be skipped because they are internal or belong to the stdlib.
var patterns = []string{
	"runtime/",
	"testing/",
	"log/slog/",
	"slogx/",
	"errorx",
}

var acceptedPatterns = []string{
	"slogx/slog_test.go",
}

// patternSkip returns true if the current frame matches the pattern to be skipped.
func patternSkip(str string) bool {
	for _, pattern := range acceptedPatterns {
		if strings.Contains(str, pattern) {
			return false
		}
	}

	for _, pattern := range patterns {
		if strings.Contains(str, pattern) {
			return true
		}
	}

	return false
}
