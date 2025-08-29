package components

import (
	"fmt"
	"github.com/joho/godotenv"
	"gps-no-sync/internal/config/shared"
	"gps-no-sync/internal/interfaces"
	"strings"
)

type InfluxConfig interface {
	interfaces.Config
	GetUrl() string
}

type InfluxConfigImpl struct {
	URL           string `json:"url"`
	Token         string `json:"token"`
	Organization  string `json:"organization"`
	Bucket        string `json:"bucket"`
	BatchSize     int    `json:"batch_size"`
	FlushInterval int    `json:"flush_interval_seconds"`
}

func NewInfluxConfig() InfluxConfigImpl {
	config := InfluxConfigImpl{}
	config.Load()
	config.SetDefaults()
	return config
}

func (I *InfluxConfigImpl) Load() {
	_ = godotenv.Load()

	I.URL = shared.GetEnv("INFLUXDB_URL")
	I.Token = shared.GetEnv("INFLUXDB_TOKEN")
	I.Organization = shared.GetEnv("INFLUXDB_ORG")
	I.Bucket = shared.GetEnv("INFLUXDB_BUCKET")
	I.BatchSize = shared.GetEnvAsInt("INFLUXDB_BATCH_SIZE")
	I.FlushInterval = shared.GetEnvAsInt("INFLUXDB_FLUSH_INTERVAL")
}

func (I *InfluxConfigImpl) SetDefaults() {
	if I.URL == "" {
		I.URL = "http://localhost:8086"
	}
	if I.Organization == "" {
		I.Organization = "gps_no_sync"
	}
	if I.Bucket == "" {
		I.Bucket = "measurements"
	}
	if I.BatchSize <= 0 {
		I.BatchSize = 100
	}
	if I.FlushInterval <= 0 {
		I.FlushInterval = 10
	}
}

func (I *InfluxConfigImpl) Validate() error {
	if I.URL == "" {
		return fmt.Errorf("influxdb url is required")
	}
	if I.Token == "" {
		return fmt.Errorf("influxdb token is required")
	}
	if I.Organization == "" {
		return fmt.Errorf("influxdb organization is required")
	}
	if I.Bucket == "" {
		return fmt.Errorf("influxdb bucket is required")
	}
	if !(I.URL[:7] == "http://" || I.URL[:8] == "https://") {
		return fmt.Errorf("influxdb url must start with http:// or https://")
	}
	if I.BatchSize <= 0 {
		return fmt.Errorf("influxdb batch size must be greater than 0")
	}
	if I.FlushInterval <= 0 {
		return fmt.Errorf("influxdb flush interval must be greater than 0")
	}
	if I.FlushInterval > 60 {
		return fmt.Errorf("influxdb flush interval must be less than or equal to 60 seconds")
	}
	if I.FlushInterval < 1 {
		return fmt.Errorf("influxdb flush interval must be greater than or equal to 1 second")
	}
	if len(I.Token) < 20 || strings.Count(I.Token, "-") < 2 {
		return fmt.Errorf("influxdb token seems to be invalid")
	}

	return nil
}

func (I *InfluxConfigImpl) GetUrl() string {
	return fmt.Sprintf("%s", I.URL)
}

var _ InfluxConfig = (*InfluxConfigImpl)(nil)
