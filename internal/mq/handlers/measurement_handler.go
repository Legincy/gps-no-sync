package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
	"time"
)

type MeasurementHandler struct {
	measurementService *services.MeasurementService
	logger             zerolog.Logger
	handlerTopic       string
	topicManager       *mq.TopicManager
}

func NewMeasurementHandler(
	measurementService *services.MeasurementService,
	logger zerolog.Logger,
	topicManager *mq.TopicManager,
) *MeasurementHandler {
	return &MeasurementHandler{
		measurementService: measurementService,
		logger:             logger,
		handlerTopic:       topicManager.GetMeasurementTopic(),
		topicManager:       topicManager,
	}
}

func (h *MeasurementHandler) TransformMessage(ctx context.Context, msg mqtt.Message) (*mq.MeasurementMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message: %w", ErrMessageIsNil)
	}

	topic := msg.Topic()
	payload := string(msg.Payload())

	if len(payload) == 0 {
		return nil, ErrEmptyMessage
	}

	var measurementMessage mq.MeasurementMessage
	if err := json.Unmarshal(msg.Payload(), &measurementMessage); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Str("payload", payload).
			Msg("Failed to parse measurement message")
		return nil, fmt.Errorf("could not parse measurement data: %w", ErrInvalidMessage)
	}

	measurementMessage.Topic = topic

	if measurementMessage.Data.StationID == "" {
		stationID := h.topicManager.ExtractStationId(topic)
		if stationID == "" {
			h.logger.Error().
				Str("topic", topic).
				Msg("Could not extract station ID from topic")
			return nil, fmt.Errorf("could not extract station ID from topic")
		}
		measurementMessage.Data.StationID = stationID
	}

	// Set timestamp if not provided
	if measurementMessage.Data.Timestamp.IsZero() {
		measurementMessage.Data.Timestamp = time.Now()
	}

	return &measurementMessage, nil
}

func (h *MeasurementHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()

	measurementMessage, err := h.TransformMessage(ctx, msg)
	if err != nil {
		if errors.Is(err, ErrEmptyMessage) {
			return
		}

		h.logger.Error().Err(err).
			Str("message", string(msg.Payload())).
			Str("topic", topic).
			Msg("Failed to transform measurement message")
		return
	}

	if err := h.measurementService.ProcessMessage(ctx, measurementMessage); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Str("station_id", measurementMessage.Data.StationID).
			Str("type", string(measurementMessage.Data.Type)).
			Msg("Failed to process measurement message")
		return
	}

	h.logger.Debug().
		Str("topic", topic).
		Str("station_id", measurementMessage.Data.StationID).
		Str("type", string(measurementMessage.Data.Type)).
		Msg("Measurement message processed successfully")
}
