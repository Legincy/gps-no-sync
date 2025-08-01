package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/models"
)

type DeviceService struct {
	deviceRepository *repositories.DeviceRepository
	logger           zerolog.Logger
}

func NewDeviceService(deviceRepository *repositories.DeviceRepository, logger zerolog.Logger) *DeviceService {
	return &DeviceService{
		deviceRepository: deviceRepository,
		logger:           logger,
	}
}

func (s *DeviceService) ProcessDeviceData(ctx context.Context, deviceID string, rawData *models.DeviceRawData) error {
	if rawData.UWB.Cluster != nil {
		s.logger.Warn().
			Str("device_id", deviceID).
			Str("mac_address", rawData.Device.MacAddress).
			Msg("found cluster - skipping device")
		return nil
	}

	if s.deviceRepository == nil {
		return fmt.Errorf("device repository is not initialized")
	}

	fmt.Printf("Processing device data for device ID: %+v\n", rawData.Device.MacAddress)

	existingDevice, err := s.deviceRepository.FindByMacAddress(ctx, rawData.Device.MacAddress)
	fmt.Printf("Existing device: %+v\n", existingDevice)
	knownDevice := err == nil

	var deviceType models.DeviceType
	switch rawData.UWB.DeviceType {
	case "TAG":
		deviceType = models.DeviceTypeTag
	case "ANCHOR":
		deviceType = models.DeviceTypeAnchor
	default:
		s.logger.Warn().
			Str("device_id", deviceID).
			Str("unknown_type", rawData.UWB.DeviceType).
			Msg("unknown device type, keeping old type")
		if knownDevice {
			deviceType = existingDevice.DeviceType
		} else {
			deviceType = models.DeviceTypeUnknown
		}
	}

	device := &models.Device{
		MacAddress: rawData.Device.MacAddress,
		Name:       rawData.Device.Name,
		DeviceType: deviceType,
	}

	if err := s.deviceRepository.CreateOrUpdate(ctx, device); err != nil {
		return fmt.Errorf("error creating or updating device: %w", err)
	}

	return nil
}
