package logger

import (
	"log/slog"
	"os"
)

type Level int

const (
	Info Level = iota
	Debug
	Warning
	Error
	Fatal
)

var (
	loggingLevel = new(slog.LevelVar)
	logger       = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
)

func EnableDebug() {
	loggingLevel.Set(slog.LevelDebug)
}

func Log(logType Level, message string, attr ...interface{}) {
	switch logType {
	case Info:
		logger.Info(message, attr...)
	case Debug:
		logger.Debug(message, attr...)
	case Warning:
		logger.Warn(message, attr...)
	case Error:
		logger.Error(message, attr...)
	case Fatal:
		logger.Error(message, attr...)
		os.Exit(1)
	default:
		logger.Info(message, attr...)
	}
}
