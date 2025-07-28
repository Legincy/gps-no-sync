package listeners

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
)

type DeviceTableListener struct {
	*BaseTableListener
	logger        zerolog.Logger
	mqttClient    *mqtt.Client
	topicManager  *mqtt.TopicManager
	deviceService *services.DeviceService
}

func NewDeviceTableListener(
	logger zerolog.Logger,
	mqttClient *mqtt.Client,
	topicManager *mqtt.TopicManager,
	deviceService *services.DeviceService,
) *DeviceTableListener {
	return &DeviceTableListener{
		BaseTableListener: NewBaseTableListener("devices"),
		logger:            logger,
		mqttClient:        mqttClient,
		topicManager:      topicManager,
		deviceService:     deviceService,
	}
}

func (d *DeviceTableListener) HandleChange(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Str("operation", event.Operation).
		Str("table", event.Table).
		Time("timestamp", event.Timestamp).
		Msg("Device table change detected")

	switch event.Operation {
	case "INSERT":
		return d.handleDeviceInsert(ctx, event)
	case "UPDATE":
		return d.handleDeviceUpdate(ctx, event)
	case "DELETE":
		return d.handleDeviceDelete(ctx, event)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

func (d *DeviceTableListener) handleDeviceInsert(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("device_data", event.NewData).
		Msg("New device created")

	topic := d.topicManager.BaseTopic + "/events/device/created"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "device_created",
		"device":    event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish device creation event")
	}

	return nil
}

func (d *DeviceTableListener) handleDeviceUpdate(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("old_data", event.OldData).
		Interface("new_data", event.NewData).
		Msg("Device updated")

	topic := d.topicManager.BaseTopic + "/events/device/updated"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "device_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish device update event")
	}

	return nil
}

func (d *DeviceTableListener) handleDeviceDelete(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("deleted_device", event.OldData).
		Msg("🗑️ Device deleted")

	topic := d.topicManager.BaseTopic + "/events/device/deleted"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":        "device_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish device deletion event")
	}

	return nil
}
