package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt2 "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
	"strings"
	"time"
)

type MeasurementMessage struct {
	Source string            `json:"source"`
	Data   []MeasurementData `json:"data"`
}

type MeasurementData struct {
	Value  float64 `json:"value"`
	Type   string  `json:"type"`
	Unit   string  `json:"unit"`
	Target string  `json:"target"`
}

type MeasurementHandler interface {
	mqtt.TopicHandler
	HandleRegister(ctx context.Context, msg *mqtt.IncomingMessage) error
}

type MeasurementHandlerImpl struct {
	measurementService *services.MeasurementService
	publisher          *mqtt.PublisherImpl
	topicManager       *mqtt.TopicManagerImpl
	logger             zerolog.Logger
}

func NewMeasurementHandler(
	measurementService *services.MeasurementService,
	publisher *mqtt.PublisherImpl,
	topicManager *mqtt.TopicManagerImpl,
	logger zerolog.Logger,
) *MeasurementHandlerImpl {
	return &MeasurementHandlerImpl{
		measurementService: measurementService,
		publisher:          publisher,
		topicManager:       topicManager,
		logger:             logger.With().Str("handler", "measurement").Logger(),
	}
}

func (m *MeasurementHandlerImpl) Process(ctx context.Context, msg mqtt2.Message) error {
	topic := msg.Topic()
	payload := msg.Payload()

	stationID, err := m.topicManager.ExtractMeasurementID(topic)
	if err != nil {
		return fmt.Errorf("invalid topic: %w", err)
	}

	var measurementMsg MeasurementMessage
	if err := json.Unmarshal(payload, &measurementMsg); err != nil {
		m.logger.Error().Err(err).
			Str("topic", topic).
			Str("station_id", stationID).
			Msg("Failed to parse measurement message")
		return fmt.Errorf("invalid JSON: %w", err)
	}

	fmt.Println(measurementMsg)

	if err := measurementMsg.Validate(); err != nil {
		m.logger.Error().Err(err).
			Str("topic", topic).
			Str("station_id", stationID).
			Msg("Invalid measurement message")
		return fmt.Errorf("validation failed: %w", err)
	}

	measurements := m.convertToMeasurements(stationID, &measurementMsg)

	m.logger.Info().
		Str("station_id", stationID).
		Str("source", measurementMsg.Source).
		Int("count", len(measurements)).
		Msg("Processing measurement batch")

	if err := m.measurementService.StoreBatch(ctx, measurements); err != nil {
		m.logger.Error().Err(err).
			Str("station_id", stationID).
			Int("count", len(measurements)).
			Msg("Failed to store measurements")
		return fmt.Errorf("storage failed: %w", err)
	}

	m.logger.Debug().
		Str("station_id", stationID).
		Int("count", len(measurements)).
		Msg("Measurements stored successfully")

	return nil
}

func (m *MeasurementMessage) Validate() error {
	if m.Source == "" {
		return fmt.Errorf("source is required")
	}

	if len(m.Data) == 0 {
		return fmt.Errorf("payload must contain at least one measurement")
	}

	for i, measurement := range m.Data {
		if measurement.Type == "" {
			return fmt.Errorf("measurement[%d]: type is required", i)
		}
		if measurement.Unit == "" {
			return fmt.Errorf("measurement[%d]: unit is required", i)
		}
	}

	return nil
}

func (h *MeasurementHandlerImpl) convertToMeasurements(
	stationID string,
	msg *MeasurementMessage,
) []*models.Measurement {
	now := time.Now()
	measurements := make([]*models.Measurement, len(msg.Data))

	for i, data := range msg.Data {
		measurements[i] = &models.Measurement{
			StationID: stationID,
			Type:      models.MeasurementType(strings.ToUpper(data.Type)),
			Value:     data.Value,
			Unit:      strings.ToUpper(data.Unit),
			Target:    normalizeMacAddress(data.Target),
			Metadata: map[string]interface{}{
				"source": normalizeMacAddress(msg.Source),
			},
			Timestamp: now,
		}
	}

	return measurements
}
