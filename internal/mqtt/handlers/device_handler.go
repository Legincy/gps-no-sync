package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	mqtt2 "gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
	"time"
)

type DeviceHandler struct {
	deviceService *services.DeviceService
	logger        zerolog.Logger
	handlerTopic  string
	topicManager  *mqtt2.TopicManager
}

func NewDeviceHandler(topicManager *mqtt2.TopicManager, deviceService *services.DeviceService, logger zerolog.Logger) *DeviceHandler {

	return &DeviceHandler{
		deviceService: deviceService,
		logger:        logger,
		topicManager:  topicManager,
		handlerTopic:  topicManager.BuildDeviceTopicSubscription(),
	}
}

func (h *DeviceHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()
	payload := msg.Payload()

	h.logger.Debug().
		Str("topic", topic).
		Str("payload", string(payload)).
		Msg("Received device update")

	deviceID, err := h.topicManager.ExtractDeviceIDFromTopic(topic, h.handlerTopic)
	if err != nil {
		h.logger.Error().Err(err).Str("topic", topic).Msg("Could not extract device ID from topic")
		return
	}

	var deviceRawData models.DeviceRawData
	if err := json.Unmarshal(payload, &deviceRawData); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Str("payload", string(payload)).
			Msg("Could not parse device raw data")
		return
	}

	if err := h.validateDeviceData(&deviceRawData); err != nil {
		h.logger.Error().Err(err).
			Str("topic", topic).
			Interface("data", deviceRawData).
			Msg("Invalid device data received")
		return
	}

	if err := h.deviceService.ProcessDeviceData(ctx, deviceID, &deviceRawData); err != nil {
		h.logger.Error().Err(err).
			Str("device_id", deviceID).
			Msg("Error processing device data")
		return
	}
}

func (h *DeviceHandler) validateDeviceData(data *models.DeviceRawData) error {
	if data.Device.MacAddress == "" {
		return fmt.Errorf("mac_address is not set")
	}
	return nil
}
