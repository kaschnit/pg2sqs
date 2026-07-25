package main

import "log/slog"

type Config struct {
	Verbosity Verbosity
	SQS       SQSConfig
}

type SQSConfig struct {
	QueueURL string `json:"queueURL"`
}

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
