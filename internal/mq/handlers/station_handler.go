package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/mq/messages"
	"gps-no-sync/internal/services"
	"time"
)

type StationHandler struct {
	stationService *services.StationService
	logger         zerolog.Logger
	handlerTopic   string
	topicManager   *mq.TopicManager
}

func NewStationHandler(topicManager *mq.TopicManager, stationService *services.StationService, logger zerolog.Logger) *StationHandler {
	return &StationHandler{
		stationService: stationService,
		logger:         logger,
		topicManager:   topicManager,
		handlerTopic:   topicManager.GetStationTopic(),
	}
}

func (h *StationHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()
	payload := msg.Payload()

	if len(payload) == 0 {
		return
	}

	topicId, err := h.topicManager.ExtractStationIdFromTopic(topic, mq.StationTopicTemplate)
	if err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Msg("Invalid station data received")
	}

	h.logger.Debug().
		Str("topic", topic).
		Str("payload", string(payload)).
		Msg("Received device update")

	var stationMessage messages.StationMessage
	if err := json.Unmarshal(payload, &stationMessage); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Str("payload", string(payload)).
			Msg("Could not parse station data")
		return
	}

	if stationMessage.Source == "SYNC" {
		h.logger.Debug().
			Str("source", stationMessage.Source).
			Msg("Ignoring station message")
		return
	}

	station, err := stationMessage.Data.ToModel()
	if err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Interface("data", stationMessage).
			Msg("Could not transform to model")
	}
	station.Topic = topicId

	if err := h.stationService.ProcessStation(ctx, &station); err != nil {
		h.logger.Error().Err(err).
			Str("mac_address", station.MacAddress).
			Msg("Error processing station data")
		return
	}
}

func (h *StationHandler) validateStationDto(dto *messages.StationMessage) error {
	if dto.Data.MacAddress == "" {
		return fmt.Errorf("mac_address is not set")
	}
	return nil
}
