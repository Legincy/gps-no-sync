package components

import (
	"fmt"
	"gps-no-sync/internal/config/shared"
	"gps-no-sync/internal/interfaces"
)

type LoggerConfig interface {
	interfaces.Config
}

type LoggerConfigImpl struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

func NewLoggerConfig() LoggerConfigImpl {
	config := LoggerConfigImpl{}
	config.Load()
	config.SetDefaults()
	return config
}

func (L *LoggerConfigImpl) Load() {
	L.Level = shared.GetEnv("LOG_LEVEL")
	L.Format = shared.GetEnv("LOG_FORMAT")
}

func (L *LoggerConfigImpl) SetDefaults() {
	if L.Level == "" {
		L.Level = "info"
	}
	if L.Format == "" {
		L.Format = "console"
	}
}

func (L *LoggerConfigImpl) Validate() error {
	if L.Level == "" {
		return fmt.Errorf("LOG_LEVEL is required")
	}

	if L.Format == "" {
		return fmt.Errorf("LOG_FORMAT is required")
	}

	return nil
}

var _ LoggerConfig = (*LoggerConfigImpl)(nil)
