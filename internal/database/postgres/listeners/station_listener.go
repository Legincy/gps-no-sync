package listeners

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
	"time"
)

type StationTableListener struct {
	*BaseTableListener
	logger            zerolog.Logger
	mqttClient        interfaces.IMqClient
	topicManager      *mq.TopicManager
	stationService    *services.StationService
	stationRepository *repositories.StationRepository
}

func NewStationTableListener(
	logger zerolog.Logger,
	mqttClient interfaces.IMqClient,
	topicManager *mq.TopicManager,
	stationService *services.StationService,
	stationRepository *repositories.StationRepository,
) *StationTableListener {
	return &StationTableListener{
		BaseTableListener: NewBaseTableListener("stations"),
		logger:            logger,
		mqttClient:        mqttClient,
		topicManager:      topicManager,
		stationService:    stationService,
		stationRepository: stationRepository,
	}
}

func (d *StationTableListener) HandleChange(ctx context.Context, event *interfaces.TableChangeEvent) error {
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

func (d *StationTableListener) handleInsert(ctx context.Context, event *interfaces.TableChangeEvent) error {
	d.logger.Info().
		Interface("station_data", event.NewData).
		Msg("New Station created")

	newData, _, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(newData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}

	normalizedStation, err := station.Standardize()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to normalize station data")
	}

	isEqual, err := station.Equals(normalizedStation)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to compare station data")
	}

	if !isEqual {
		err := d.stationRepository.CreateOrUpdate(ctx, normalizedStation)
		if err != nil {
			return err
		}

		return nil
	}

	err = d.stationService.SyncToMqtt(ctx, normalizedStation)
	if err != nil {
		d.logger.Error().Err(err).
			Msg("Failed to sync station to MQTT after creation")
	}

	baseTopic := d.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/stations/created"
	if err := d.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":     "station_created",
		"station":   event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station creation event")
	}

	return nil
}

func (d *StationTableListener) handleUpdate(ctx context.Context, event *interfaces.TableChangeEvent) error {
	d.logger.Info().
		Interface("old_data", event.OldData).
		Interface("new_data", event.NewData).
		Msg("Station updated")

	newData, _, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(newData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}

	err = d.stationService.SyncToMqtt(ctx, station)
	if err != nil {
		d.logger.Error().Err(err).
			Msg("Failed to sync station to MQTT after update")
	}

	baseTopic := d.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/stations/updated"
	if err := d.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":     "station_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station update event")
	}

	return nil
}

func (d *StationTableListener) handleDelete(ctx context.Context, event *interfaces.TableChangeEvent) error {
	d.logger.Info().
		Interface("deleted_station", event.OldData).
		Msg("Station deleted")

	_, oldData, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(oldData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}
	station.DeletedAt = time.Now()

	err = d.stationService.SyncToMqtt(ctx, station)
	if err != nil {
		d.logger.Error().Err(err).
			Msg("Failed to sync station to MQTT after creation")
	}

	baseTopic := d.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/stations/deleted"
	if err := d.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":        "station_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		d.logger.Error().Err(err).Msg("Failed to publish station deletion event")
	}

	return nil
}
