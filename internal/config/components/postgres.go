package components

import (
	"fmt"
	"github.com/joho/godotenv"
	"gps-no-sync/internal/config/shared"
	"gps-no-sync/internal/interfaces"
)

type PostgresConfig interface {
	interfaces.Config
	GetDsn() string
}

type PostgresConfigImpl struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	SSLMode  string `json:"ssl_mode"`
	TimeZone string `json:"timezone"`
}

func NewPostgresConfig() PostgresConfigImpl {
	config := PostgresConfigImpl{}
	config.Load()
	config.SetDefaults()
	return config
}

func (P *PostgresConfigImpl) Load() {
	_ = godotenv.Load()

	P.Host = shared.GetEnv("POSTGRES_HOST")
	P.Port = shared.GetEnvAsInt("POSTGRES_PORT")
	P.User = shared.GetEnv("POSTGRES_USER")
	P.Password = shared.GetEnv("POSTGRES_PASSWORD")
	P.Database = shared.GetEnv("POSTGRES_DB")
	P.SSLMode = shared.GetEnv("POSTGRES_SSL_MODE")
	P.TimeZone = shared.GetEnv("TZ")
}

func (P *PostgresConfigImpl) SetDefaults() {
	if P.Host == "" {
		P.Host = "localhost"
	}
	if P.Port == 0 {
		P.Port = 5432
	}
	if P.User == "" {
		P.User = "postgres"
	}
	if P.Database == "" {
		P.Database = "gps_no"
	}
	if P.SSLMode == "" {
		P.SSLMode = "disable"
	}
	if P.TimeZone == "" {
		P.TimeZone = "UTC"
	}
}

func (P *PostgresConfigImpl) Validate() error {
	if P.Host == "" {
		return fmt.Errorf("POSTGRES_HOST is required")
	}
	if P.Port == 0 {
		return fmt.Errorf("POSTGRES_PORT is required")
	}
	if P.User == "" {
		return fmt.Errorf("POSTGRES_USER is required")
	}
	if P.Database == "" {
		return fmt.Errorf("POSTGRES_DB is required")
	}
	if P.SSLMode != "disable" && P.SSLMode != "require" && P.SSLMode != "verify-ca" && P.SSLMode != "verify-full" && P.SSLMode != "true" && P.SSLMode != "false" {
		return fmt.Errorf("POSTGRES_SSL_MODE must be one of: disable, require, verify-ca, verify-full")
	}
	return nil
}

func (P *PostgresConfigImpl) GetDsn() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&TimeZone=%s", P.User, P.Password, P.Host, P.Port, P.Database, P.SSLMode, P.TimeZone)
}

var _ PostgresConfig = (*PostgresConfigImpl)(nil)
