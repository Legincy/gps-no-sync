package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/influxdb"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"time"
)

type MeasurementService struct {
	influxDB     *influxdb.InfluxDB
	topicManager *mq.TopicManager
	logger       zerolog.Logger
}

func NewMeasurementService(
	influxDB *influxdb.InfluxDB,
	topicManager *mq.TopicManager,
	logger zerolog.Logger,
) *MeasurementService {
	return &MeasurementService{
		influxDB:     influxDB,
		topicManager: topicManager,
		logger:       logger,
	}
}

func (s *MeasurementService) ProcessMessage(ctx context.Context, measurementMessage *mq.MeasurementMessage) error {
	if measurementMessage.Source == "SYNC" {
		return nil
	}

	measurement := measurementMessage.Data
	measurement.ReceivedAt = time.Now()

	if measurement.StationID == "" {
		measurement.StationID = s.topicManager.ExtractStationId(measurementMessage.Topic)
	}

	if err := measurement.Validate(); err != nil {
		s.logger.Error().Err(err).
			Str("topic", measurementMessage.Topic).
			Str("station_id", measurement.StationID).
			Msg("Invalid measurement received")
		return fmt.Errorf("invalid measurement: %w", err)
	}

	if err := s.StoreMeasurement(ctx, &measurement); err != nil {
		s.logger.Error().Err(err).
			Str("station_id", measurement.StationID).
			Str("type", string(measurement.Type)).
			Msg("Failed to store measurement")
		return fmt.Errorf("failed to store measurement: %w", err)
	}

	s.logger.Debug().
		Str("station_id", measurement.StationID).
		Str("type", string(measurement.Type)).
		Time("timestamp", measurement.Timestamp).
		Msg("Measurement processed successfully")

	return nil
}

func (s *MeasurementService) StoreMeasurement(ctx context.Context, measurement *models.Measurement) error {
	tags := measurement.GetTags()
	fields := measurement.GetFields()
	measurementName := string(measurement.Type)

	s.logger.Info().
		Str("measurement_name", measurementName).
		Interface("tags", tags).
		Interface("fields", fields).
		Time("timestamp", measurement.Timestamp).
		Msg("About to store measurement in InfluxConfig")

	err := s.influxDB.WriteMeasurementSync("measurement", measurementName, tags, fields, measurement.Timestamp)
	if err != nil {
		s.logger.Error().Err(err).
			Str("measurement_name", measurementName).
			Msg("Failed to write measurement to InfluxConfig")
		return fmt.Errorf("failed to write measurement to InfluxConfig: %w", err)
	}

	s.logger.Info().
		Str("measurement_name", measurementName).
		Msg("Successfully stored measurement in InfluxConfig")

	return nil
}
