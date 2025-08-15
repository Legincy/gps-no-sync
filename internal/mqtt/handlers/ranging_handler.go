package handlers

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	mqtt2 "gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type RangingHandler struct {
	rangingService *services.RangingService
	logger         zerolog.Logger
	handlerTopic   string
	topicManager   *mqtt2.TopicManager
}

func NewRangingHandler(topicManager *mqtt2.TopicManager, rangingService *services.RangingService, logger zerolog.Logger) *RangingHandler {

	return &RangingHandler{
		rangingService: rangingService,
		logger:         logger,
		topicManager:   topicManager,
		handlerTopic:   topicManager.GetUwbRangingTopic(),
	}
}

func (h *RangingHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()
	payload := msg.Payload()

	h.logger.Debug().
		Str("topic", topic).
		Str("payload", string(payload)).
		Msg("Ranging Data empfangen")

	deviceID, err := h.topicManager.ExtractDeviceIDFromTopic(topic, h.handlerTopic)
	if err != nil {
		h.logger.Error().Err(err).Str("topic", topic).Msg("Could not extract device ID from topic")
		return
	}

	var rangingDataArray models.RangingDataArray
	if err := json.Unmarshal(payload, &rangingDataArray); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Str("payload", string(payload)).
			Msg("Konnte Ranging Data nicht parsen")
		return
	}

	if err := h.rangingService.ProcessRangingData(ctx, deviceID, rangingDataArray); err != nil {
		h.logger.Error().Err(err).
			Str("device_id", deviceID).
			Int("measurements", len(rangingDataArray)).
			Msg("Fehler beim Verarbeiten der Ranging Daten")
		return
	}

	h.logger.Info().
		Str("device_id", deviceID).
		Int("measurements", len(rangingDataArray)).
		Msg("Ranging Daten erfolgreich verarbeitet")
}
