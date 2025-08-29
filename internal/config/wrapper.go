package config

import (
	"github.com/joho/godotenv"
	"gps-no-sync/internal/config/components"
)

type Wrapper interface {
	GetMQTTConfig() components.MQTTConfigImpl
	GetPostgresConfig() components.PostgresConfigImpl
	GetInfluxConfig() components.InfluxConfigImpl
	GetLoggerConfig() components.LoggerConfigImpl
	GetServiceConfig() components.ServiceConfigImpl
}

type WrapperImpl struct {
	MQTTConfig     components.MQTTConfigImpl     `json:"mqtt"`
	PostgresConfig components.PostgresConfigImpl `json:"postgres"`
	InfluxConfig   components.InfluxConfigImpl   `json:"influx"`
	LoggerConfig   components.LoggerConfigImpl   `json:"logger"`
	ServiceConfig  components.ServiceConfigImpl  `json:"service"`
}

func NewWrapper() WrapperImpl {
	mqttConfig := components.NewMQTTConfig()
	postgresConfig := components.NewPostgresConfig()
	influxConfig := components.NewInfluxConfig()
	loggerConfig := components.NewLoggerConfig()
	serviceConfig := components.NewServiceConfig()

	return WrapperImpl{
		MQTTConfig:     mqttConfig,
		PostgresConfig: postgresConfig,
		InfluxConfig:   influxConfig,
		LoggerConfig:   loggerConfig,
		ServiceConfig:  serviceConfig,
	}
}

func (C WrapperImpl) Load() {
	_ = godotenv.Load()

	C.MQTTConfig.Load()
	C.PostgresConfig.Load()
	C.InfluxConfig.Load()
	C.LoggerConfig.Load()
	C.ServiceConfig.Load()
}
