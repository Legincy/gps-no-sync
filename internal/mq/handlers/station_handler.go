package handlers

import (
	"bytes"
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

var (
	ErrEmptyMessage       = errors.New("empty message received")
	ErrSyncMessageIgnored = errors.New("sync message ignored")
	ErrInvalidMessage     = errors.New("invalid message format")
	ErrValidationFailed   = errors.New("validation failed")
	ErrStationNotFound    = errors.New("station not found")
	ErrMessageIsNil       = errors.New("message is nil")
)

type StationHandler struct {
	stationService *services.StationService
	logger         zerolog.Logger
	handlerTopic   string
	topicManager   *mq.TopicManager
}

func NewStationHandler(
	stationService *services.StationService,
	logger zerolog.Logger,
	topicManager *mq.TopicManager,
) *StationHandler {
	return &StationHandler{
		stationService: stationService,
		logger:         logger,
		handlerTopic:   topicManager.GetStationTopic(),
		topicManager:   topicManager,
	}
}

func (h *StationHandler) TransformMessage(ctx context.Context, msg mqtt.Message) (*mq.StationMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message: %w", ErrMessageIsNil)
	}

	payload := string(msg.Payload())

	if len(payload) == 0 {
		return nil, ErrEmptyMessage
	}

	var stationMessage mq.StationMessage
	if err := json.Unmarshal(msg.Payload(), &stationMessage); err != nil {
		//TODO: Sync just the station that failed validation (same for clusters)
		err := h.stationService.SyncAll(ctx)
		if err != nil {
			h.logger.Error().Err(err).
				Str("topic", msg.Topic()).
				Msg("Failed to sync all stations after invalid message")
			return nil, fmt.Errorf("failed to sync stations: %w", err)
		}

		return nil, fmt.Errorf("could not parse station data: %w", ErrInvalidMessage)
	}
	stationMessage.Topic = msg.Topic()

	byteStationMessage, err := json.Marshal(stationMessage)
	if err != nil {
		h.logger.Error().Err(err).
			Str("topic", msg.Topic()).
			Msg("Failed to marshal station message")
		return nil, fmt.Errorf("failed to marshal station message: %w", err)
	}

	if !bytes.Equal(msg.Payload(), byteStationMessage) {
		err := h.stationService.SyncAll(ctx)
		if err != nil {
			h.logger.Error().Err(err).
				Str("topic", msg.Topic()).
				Msg("Failed to sync all stations after invalid message")
		}
	}

	return &stationMessage, nil
}

func (h *StationHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()

	stationMessage, err := h.TransformMessage(ctx, msg)
	if err != nil {
		if errors.Is(err, ErrEmptyMessage) {
			return
		}

		h.logger.Error().Err(err).
			Str("message", string(msg.Payload())).
			Str("topic", topic).
			Msg("Failed to transform message")
		return
	}

	h.stationService.ProcessMessage(ctx, stationMessage)
}
