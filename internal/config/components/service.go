package components

import (
	"fmt"
	"gps-no-sync/internal/config/shared"
	"gps-no-sync/internal/interfaces"
	"time"
)

type ServiceConfig interface {
	interfaces.Config
}

type ServiceConfigImpl struct {
	Name                    string        `json:"name"`
	Version                 string        `json:"version"`
	DeviceUpdateInterval    time.Duration `json:"device_update_interval"`
	DeviceTimeoutDuration   time.Duration `json:"device_timeout_duration"`
	MaxConcurrentProcessing int           `json:"max_concurrent_processing"`
}

func NewServiceConfig() ServiceConfigImpl {
	return ServiceConfigImpl{}
}

func (S *ServiceConfigImpl) Load() {
	S.Name = shared.GetEnv("SERVICE_NAME")
	S.Version = shared.GetEnv("SERVICE_VERSION")
	S.DeviceUpdateInterval = shared.GetEnvAsDuration("DEVICE_UPDATE_INTERVAL")
	S.DeviceTimeoutDuration = shared.GetEnvAsDuration("DEVICE_TIMEOUT_DURATION")
	S.MaxConcurrentProcessing = shared.GetEnvAsInt("MAX_CONCURRENT_PROCESSING")
}

func (S *ServiceConfigImpl) SetDefaults() {
	if S.Name == "" {
		S.Name = "gps-no-sync"
	}
	if S.Version == "" {
		S.Version = "1.0.0"
	}
	if S.DeviceUpdateInterval <= 0 {
		S.DeviceUpdateInterval = 30 * time.Second
	}
	if S.DeviceTimeoutDuration <= 0 {
		S.DeviceTimeoutDuration = 5 * time.Minute
	}
	if S.MaxConcurrentProcessing <= 0 {
		S.MaxConcurrentProcessing = 10
	}

}

func (S *ServiceConfigImpl) Validate() error {
	if S.Name == "" {
		return fmt.Errorf("Service name is required")
	}

	if S.Version == "" {
		return fmt.Errorf("Service version is required")
	}

	if S.DeviceUpdateInterval <= 0 {
		return fmt.Errorf("DEVICE_UPDATE_INTERVAL must be greater than 0")
	}

	if S.DeviceTimeoutDuration <= 0 {
		return fmt.Errorf("DEVICE_TIMEOUT_DURATION must be greater than 0")
	}

	if S.MaxConcurrentProcessing <= 0 {
		return fmt.Errorf("MAX_CONCURRENT_PROCESSING must be greater than 0")
	}

	return nil
}

var _ ServiceConfig = (*ServiceConfigImpl)(nil)
