package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gps-no-sync/internal/config"
	"os"
	"strings"
	"time"
)

func NewLogger(cfg config.LoggerConfig) zerolog.Logger {
	level := zerolog.InfoLevel
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	case "fatal":
		level = zerolog.FatalLevel
	}

	var output zerolog.ConsoleWriter
	if cfg.Format == "console" {
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		}
		log.Logger = zerolog.New(output).Level(level).With().Timestamp().Logger()
	} else {
		log.Logger = zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
	}

	return log.Logger
}

func GetLogger(component string) zerolog.Logger {
	return log.With().Str("component", component).Logger()
}
