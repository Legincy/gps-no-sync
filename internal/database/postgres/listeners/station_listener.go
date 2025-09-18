package listeners

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/services"
	"time"
)

type StationTableListener struct {
	*BaseTableListener
	logger            zerolog.Logger
	stationService    *services.StationService
	stationRepository *repositories.StationRepository
}

func NewStationTableListener(
	logger zerolog.Logger,
	stationService *services.StationService,
	stationRepository *repositories.StationRepository,
) *StationTableListener {
	return &StationTableListener{
		BaseTableListener: NewBaseTableListener("stations"),
		logger:            logger,
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
	newData, _, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(newData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}

	return nil
}

func (d *StationTableListener) handleUpdate(ctx context.Context, event *interfaces.TableChangeEvent) error {
	newData, _, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(newData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}

	return nil
}

func (d *StationTableListener) handleDelete(ctx context.Context, event *interfaces.TableChangeEvent) error {
	_, oldData, err := event.GetData()
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	station := &models.Station{}
	err = json.Unmarshal(oldData, &station)
	if err != nil {
		d.logger.Error().Err(err).Msg("Failed to marshal station data")
	}
	now := time.Now()
	station.DeletedAt = &now

	return nil
}
