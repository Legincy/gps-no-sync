package listeners

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
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

func NewStationTableListener(
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
		Str("operation", string(event.Operation)).
		Str("table", event.Table).
		Time("timestamp", event.Timestamp).
		Msg("Station table change detected")

	switch event.Operation {
	case "INSERT":
		return d.handleInsert(ctx, event)
	case "UPDATE":
		return d.handleUpdate(ctx, event)
	case "DELETE":
		return d.handleDelete(ctx, event)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

func (d *StationTableListener) handleInsert(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("station_data", event.NewData).
		Msg("New Station created")

	topic := d.topicManager.BaseTopic + "/events/stations/created"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "station_created",
		"station":   event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station creation event")
	}

	return nil
}

func (d *StationTableListener) handleUpdate(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("old_data", event.OldData).
		Interface("new_data", event.NewData).
		Msg("Station updated")

	newData, err := json.Marshal(event.NewData)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal new data")

		return err
	}

	var station models.Station
	if err := json.Unmarshal(newData, &station); err != nil {
		d.logger.Error().Err(err).
			Msg("Could not parse station data")
		return err
	}

	err = d.stationService.SyncToMqtt(&station)
	if err != nil {
		d.logger.Error().Err(err).
			Str("mac_address", station.MacAddress).
			Msg("Failed to sync station to MQTT after update")
		return err
	}

	topic := d.topicManager.BaseTopic + "/events/stations/updated"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "station_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station update event")
	}

	return nil
}

func (d *StationTableListener) handleDelete(ctx context.Context, event *TableChangeEvent) error {
	d.logger.Info().
		Interface("deleted_station", event.OldData).
		Msg("Station deleted")

	topic := d.topicManager.BaseTopic + "/events/stations/deleted"
	if err := d.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":        "station_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station deletion event")
	}

	return nil
}
