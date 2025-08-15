package listeners

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
)

type StationTableListener struct {
	*BaseTableListener
	logger         zerolog.Logger
	mqttClient     *mq.Client
	topicManager   *mq.TopicManager
	stationService *services.StationService
}

func NewDeviceTableListener(
	logger zerolog.Logger,
	mqttClient *mq.Client,
	topicManager *mq.TopicManager,
	stationService *services.StationService,
) *StationTableListener {
	return &StationTableListener{
		BaseTableListener: NewBaseTableListener("stations"),
		logger:            logger,
		mqttClient:        mqttClient,
		topicManager:      topicManager,
		stationService:    stationService,
	}
}

func (d *StationTableListener) HandleChange(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Str("operation", event.Operation).
		Str("table", event.Table).
		Time("timestamp", event.Timestamp).
		Msg("Device table change detected")

	switch event.Operation {
	case "INSERT":
		return d.handleStationInsert(ctx, event)
	case "UPDATE":
		return d.handleStationUpdate(ctx, event)
	case "DELETE":
		return d.handleStationDelete(ctx, event)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

func (d *StationTableListener) handleStationInsert(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("device_data", event.NewData).
		Msg("New device created")

	topic := d.topicManager.BaseTopic + "/events/stations/created"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "device_created",
		"station":   event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station creation event")
	}

	return nil
}

func (d *StationTableListener) handleStationUpdate(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("old_data", event.OldData).
		Interface("new_data", event.NewData).
		Msg("Station updated")

	topic := d.topicManager.BaseTopic + "/events/stations/updated"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "device_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station update event")
	}

	return nil
}

func (d *StationTableListener) handleStationDelete(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("deleted_station", event.OldData).
		Msg("Station deleted")

	topic := d.topicManager.BaseTopic + "/events/stations/deleted"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":        "device_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station deletion event")
	}

	return nil
}
