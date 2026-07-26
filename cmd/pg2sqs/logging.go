package main

import "log/slog"

type Verbosity = int

func LogLevel(v Verbosity) slog.Level {
	switch {
	case v <= -2:
		return slog.LevelError
	case v <= -1:
		return slog.LevelWarn
	case v <= 0:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}
