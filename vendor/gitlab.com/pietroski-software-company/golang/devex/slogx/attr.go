package slogx

import (
	"log/slog"
)

type SlogFnAttr func(groups []string, attr slog.Attr) slog.Attr

// extraReplaceAttrs sets extra default slogx replace attributes from replaceAttrs list
func extraReplaceAttrs(extraFnAttrs ...SlogFnAttr) SlogFnAttr {
	var fnAttrs []SlogFnAttr

	fnAttrs = append(fnAttrs, extraFnAttrs...)

	return func(groups []string, attr slog.Attr) slog.Attr {
		for _, replaceAttr := range fnAttrs {
			attr = replaceAttr(groups, attr)
		}

		return attr
	}
}

// defaultReplaceAttrs sets the default slogx replace attributes from replaceAttrs list
func defaultReplaceAttrs() SlogFnAttr {
	var fnAttrs []SlogFnAttr

	fnAttrs = append(fnAttrs, replaceAttrs...)

	return func(groups []string, attr slog.Attr) slog.Attr {
		for _, replaceAttr := range fnAttrs {
			attr = replaceAttr(groups, attr)
		}

		return attr
	}
}

var replaceAttrs = []SlogFnAttr{
	replaceLogLevel,
	replaceSrcAttr,
	replaceErrAttr,
}

// replaceLogLevel replaces the log level
func replaceLogLevel(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key == slog.LevelKey {
		level := attr.Value.Any().(slog.Level)
		levelLabel, ok := LevelNames[level]
		if !ok {
			levelLabel = level.String()
		}

		attr.Value = slog.StringValue(levelLabel)
	}

	return attr
}

// SourceKey const to replace the default slog source key trace path
const SourceKey = "source"

// replaceSrcAttr replaces the source attribute for the correct one if AddSource is set to true
func replaceSrcAttr(_ []string, attr slog.Attr) slog.Attr {
	if attr.Key == SourceKey {
		attr.Value = fmtSrc(attr.Value.Any())
	}

	return attr
}

func replaceErrAttr(_ []string, attr slog.Attr) slog.Attr {
	switch attr.Value.Kind() {
	case slog.KindAny:
		switch v := attr.Value.Any().(type) {
		case error:
			attr.Value = fmtErr(v)
		}
	default:
	}

	return attr
}

type Attr any

// processAttrs process Attr and transform them to slog.Attr
func processAttrs(Attr []Attr) []slog.Attr {
	attrCount := len(Attr)
	if attrCount%2 != 0 {
		//return nil, errors.New("attrs must be key-value pairs")
		panic("invalid attr count")
	}

	var attrs []slog.Attr
	for i := 1; i < attrCount; i += 2 {
		k, ok := Attr[i-1].(string)
		if !ok {
			//return nil, errors.New("invalid key")
			panic("invalid attr key")
		}

		_ = Attr[i]
		attrs = append(attrs, slog.Any(k, Attr[i]))
	}

	return attrs
}
