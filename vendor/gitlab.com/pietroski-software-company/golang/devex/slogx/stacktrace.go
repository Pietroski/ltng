package slogx

import (
	"math"
	"path/filepath"
	"runtime"
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
	n := runtime.Callers(16, pc) // 0 - 16
	if n == 0 {
		// No PCs available. This can happen if the first argument to
		// runtime.Callers are large.
		//
		// Return now to avoid processing the zero Frame that would
		// otherwise be returned by frames.Next below.
		return nil
	}

	pc = pc[:n-1] // pc[:n] // pass only valid pcs to runtime.CallersFrames

	frames := runtime.CallersFrames(pc)

	var frameList []*Frame
	for {
		frame, more := frames.Next()
		if !more { // checks if the item is valid right away. If it is not, it does not append the invalid frame.
			break
		}

		// Process this frame.
		frameItem := Frame{
			Function: filepath.Base(frame.Function),
			File:     frame.File,
			Line:     frame.Line,
		}
		frameList = append(frameList, &frameItem)
	}

	return frameList
}
