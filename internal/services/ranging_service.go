package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/influx"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/models"
	"time"
)

type RangingService struct {
	deviceRepository *repositories.DeviceRepository
	rangingWriter    *influx.RangingWriter
	logger           zerolog.Logger
}

func NewRangingService(
	deviceRepository *repositories.DeviceRepository,
	rangingWriter *influx.RangingWriter,
	logger zerolog.Logger,
) *RangingService {
	return &RangingService{
		deviceRepository: deviceRepository,
		rangingWriter:    rangingWriter,
		logger:           logger,
	}
}

func (s *RangingService) ProcessRangingData(ctx context.Context, senderDeviceID string, rangingData models.RangingDataArray) error {
	timestamp := time.Now()
	successCount := 0

	for _, ranging := range rangingData {
		if err := s.validateRangingData(&ranging); err != nil {
			s.logger.Warn().Err(err).
				Str("source", ranging.SourceDevice).
				Str("destination", ranging.DestinationDevice).
				Msg("no valid Ranging Data")
			continue
		}

		s.updateDeviceActivity(ctx, ranging.SourceDevice, ranging.DestinationDevice)

		measurement := &models.RangingData{
			SourceDevice:      ranging.SourceDevice,
			DestinationDevice: ranging.DestinationDevice,
			Distance: struct {
				RawDistance float64 `json:"raw_distance"`
			}{
				RawDistance: ranging.Distance.RawDistance,
			},
			Timestamp: timestamp,
		}

		if err := s.rangingWriter.WriteRangingMeasurement(ctx, measurement); err != nil {
			s.logger.Error().Err(err).
				Str("source", ranging.SourceDevice).
				Str("destination", ranging.DestinationDevice).
				Msg("error writing ranging data to InfluxDB")
			continue
		}

		successCount++
	}

	return nil
}

func (s *RangingService) validateRangingData(ranging *models.RangingData) error {
	if ranging.SourceDevice == "" {
		return fmt.Errorf("source_device is not set")
	}
	if ranging.DestinationDevice == "" {
		return fmt.Errorf("destination_device is not set")
	}
	if ranging.SourceDevice == ranging.DestinationDevice {
		return fmt.Errorf("source and destination address are identical: %s", ranging.SourceDevice)
	}

	return nil
}

func (s *RangingService) updateDeviceActivity(ctx context.Context, sourceDevice, destinationDevice string) {
	if err := s.deviceRepository.UpdateLastSeen(ctx, sourceDevice); err != nil {
		s.logger.Debug().
			Str("device", sourceDevice).
			Msg("could not update last seen for source device")
	}

	if err := s.deviceRepository.UpdateLastSeen(ctx, destinationDevice); err != nil {
		s.logger.Debug().
			Str("device", destinationDevice).
			Msg("could not update last seen for destination device")
	}
}
