package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MQTT     MQTTConfig     `json:"mqtt"`
	Postgres PostgresConfig `json:"postgres"`
	InfluxDB InfluxConfig   `json:"influxdb"`
	Logger   LoggerConfig   `json:"logger"`
	Service  ServiceConfig  `json:"service"`
}

type MQTTConfig struct {
	Host                 string        `json:"host"`
	Port                 int           `json:"port"`
	Username             string        `json:"username"`
	Password             string        `json:"password"`
	ClientID             string        `json:"client_id"`
	BaseTopic            string        `json:"base_topic"`
	QoS                  byte          `json:"qos"`
	KeepAlive            int           `json:"keep_alive"`
	AutoReconnect        bool          `json:"auto_reconnect"`
	MaxReconnectInterval time.Duration `json:"max_reconnect_interval"`
	CleanSession         bool          `json:"clean_session"`
}

type PostgresConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Dsn      string `json:"dsn"`
	Database string `json:"database"`
	SSLMode  string `json:"ssl_mode"`
	TimeZone string `json:"timezone"`
}

type InfluxConfig struct {
	URL           string `json:"url"`
	Token         string `json:"token"`
	Organization  string `json:"organization"`
	Bucket        string `json:"bucket"`
	BatchSize     int    `json:"batch_size"`
	FlushInterval int    `json:"flush_interval_seconds"`
}

type LoggerConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

type ServiceConfig struct {
	Name                    string        `json:"name"`
	Version                 string        `json:"version"`
	DeviceUpdateInterval    time.Duration `json:"device_update_interval"`
	DeviceTimeoutDuration   time.Duration `json:"device_timeout_duration"`
	MaxConcurrentProcessing int           `json:"max_concurrent_processing"`
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	config := &Config{
		MQTT: MQTTConfig{
			Host:                 getEnv("MQTT_HOST", "localhost"),
			Port:                 getEnvAsInt("MQTT_PORT", 1883),
			Username:             getEnv("MQTT_USERNAME", ""),
			Password:             getEnv("MQTT_PASSWORD", ""),
			ClientID:             getEnv("MQTT_CLIENT_ID", "gps-no-sync"),
			BaseTopic:            getEnv("MQTT_BASE_TOPIC", "gpsno/devices"),
			QoS:                  byte(getEnvAsInt("MQTT_QOS", 1)),
			KeepAlive:            getEnvAsInt("MQTT_KEEP_ALIVE", 60),
			AutoReconnect:        getEnvAsBool("MQTT_AUTO_RECONNECT", true),
			MaxReconnectInterval: getEnvAsDuration("MQTT_MAX_RECONNECT_INTERVAL", "10s"),
			CleanSession:         getEnvAsBool("MQTT_CLEAN_SESSION", true),
		},
		Postgres: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     getEnvAsInt("POSTGRES_PORT", 5432),
			User:     getEnv("POSTGRES_USER", "postgres"),
			Password: getEnv("POSTGRES_PASSWORD", ""),
			Database: getEnv("POSTGRES_DATABASE", "gps_no"),
			SSLMode:  getEnv("POSTGRES_SSL_MODE", "disable"),
			TimeZone: getEnv("POSTGRES_TIMEZONE", "UTC"),
		},
		InfluxDB: InfluxConfig{
			URL:           getEnv("INFLUXDB_URL", "http://localhost:8086"),
			Token:         getEnv("INFLUXDB_TOKEN", ""),
			Organization:  getEnv("INFLUXDB_ORG", "gps_no_sync"),
			Bucket:        getEnv("INFLUXDB_BUCKET", "measurements"),
			BatchSize:     getEnvAsInt("INFLUXDB_BATCH_SIZE", 100),
			FlushInterval: getEnvAsInt("INFLUXDB_FLUSH_INTERVAL", 10),
		},
		Logger: LoggerConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "console"),
		},
		Service: ServiceConfig{
			Name:                    getEnv("SERVICE_NAME", "gps-no-sync"),
			Version:                 getEnv("SERVICE_VERSION", "1.0.0"),
			DeviceUpdateInterval:    getEnvAsDuration("DEVICE_UPDATE_INTERVAL", "30s"),
			DeviceTimeoutDuration:   getEnvAsDuration("DEVICE_TIMEOUT_DURATION", "5m"),
			MaxConcurrentProcessing: getEnvAsInt("MAX_CONCURRENT_PROCESSING", 10),
		},
	}

	baseTopic, found := strings.CutSuffix(config.MQTT.BaseTopic, "/")
	if found {
		config.MQTT.BaseTopic = baseTopic
	}

	config.Postgres.Dsn = fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
		config.Postgres.Host, config.Postgres.Port, config.Postgres.User, config.Postgres.Password, config.Postgres.Database,
		func() string {
			if config.Postgres.SSLMode == "false" || config.Postgres.SSLMode == "" {
				return "disable"
			}
			return config.Postgres.SSLMode
		}(),
	)

	return config, config.validate()
}

func (c *Config) validate() error {
	/*
		if c.MQTT.Host == "" {
			return fmt.Errorf("MQTT_HOST has to be set")
		}
		if c.Postgres.Host == "" {
			return fmt.Errorf("POSTGRES_HOST has to be set")
		}
		if c.InfluxDB.URL == "" {
			return fmt.Errorf("INFLUXDB_URL gas to be set")
		}
		if c.InfluxDB.Token == "" {
			return fmt.Errorf("INFLUXDB_TOKEN has to be set")
		}
	*/
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue string) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	duration, _ := time.ParseDuration(defaultValue)
	return duration
}
