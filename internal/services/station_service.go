package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"strings"
)

type StationService struct {
	stationRepository *repositories.StationRepository
	clusterRepository *repositories.ClusterRepository
	clusterService    *ClusterService
	client            *mq.Client
	topicManager      *mq.TopicManager
	logger            zerolog.Logger
}

func NewStationService(stationRepository *repositories.StationRepository, clusterService *ClusterService, clusterRepository *repositories.ClusterRepository, client *mq.Client, topicManager *mq.TopicManager, logger zerolog.Logger) *StationService {
	return &StationService{
		stationRepository: stationRepository,
		clusterRepository: clusterRepository,
		clusterService:    clusterService,
		client:            client,
		topicManager:      topicManager,
		logger:            logger,
	}
}

func (s *StationService) IsValidStation(station *models.Station) bool {
	if station == nil {
		return false
	}

	if station.MacAddress == "" {
		return false
	}

	return true
}

func (s *StationService) ProcessMessage(ctx context.Context, stationMessage *mq.StationMessage) {
	if stationMessage.Source == "SYNC" {
		return
	}

	stationDto := stationMessage.Data

	if stationDto.ClusterID != nil && *stationDto.ClusterID > 0 {
		dbCluster, err := s.clusterRepository.FindById(ctx, *stationDto.ClusterID)
		if err != nil {
			s.logger.Error().Err(err).
				Str("cluster_id", fmt.Sprintf("%d", *stationDto.ClusterID)).
				Msg("Failed to find cluster for station")

			stationDto.ClusterID = nil
		} else {
			stationDto.ClusterID = &dbCluster.ID
		}
	} else {
		stationDto.ClusterID = nil
	}

	var syncStation *models.Station

	dbStation, _ := s.stationRepository.FindByMacAddress(ctx, stationDto.MacAddress)
	if dbStation == nil {
		station := stationDto.ToStation()
		if !station.IsValid() {
			station.LoadDefault()
		}

		station.Topic = s.topicManager.ExtractStationId(stationMessage.Topic)

		err := s.stationRepository.Create(ctx, station)
		if err != nil {
			s.logger.Error().Err(err).
				Str("mac_address", station.MacAddress).
				Msg("Failed to create station in database")
			return
		}

		syncStation = station
	} else {
		if !dbStation.IsEqual(stationDto) {
			dbStation.UpdateFromDto(&stationDto)
			err := s.stationRepository.Update(ctx, dbStation)
			if err != nil {
				s.logger.Error().Err(err).
					Str("mac_address", dbStation.MacAddress).
					Msg("Failed to update station in database")
				return
			}
		}
		syncStation = dbStation
	}

	err := s.SyncToMqtt(ctx, syncStation)
	if err != nil {
		s.logger.Error().Err(err).
			Str("mac_address", stationDto.MacAddress).
			Msg("Failed to sync updated station to MQTTConfig")
		return
	}
}

func (s *StationService) ProcessDbUpdate(ctx context.Context, station *models.Station) error {
	err := s.SyncToMqtt(ctx, station)
	if err != nil {
		s.logger.Error().Err(err).
			Str("mac_address", station.MacAddress).
			Msg("Failed to sync updated station to MQTTConfig")
		return fmt.Errorf("error syncing updated station to MQTTConfig: %w", err)
	}

	return nil
}

func (s *StationService) ProcessDbDelete(ctx context.Context, station *models.Station) error {
	err := s.SyncToMqtt(ctx, station)
	if err != nil {
		s.logger.Error().Err(err).
			Str("mac_address", station.MacAddress).
			Msg("Failed to sync updated station to MQTTConfig")
		return fmt.Errorf("error syncing updated station to MQTTConfig: %w", err)
	}

	return nil
}

func (s *StationService) ProcessDbCreate(ctx context.Context, station *models.Station) error {
	if !station.IsValid() {
		fmt.Println(station)
		station.LoadDefault()

		err := s.stationRepository.Update(ctx, station)
		if err != nil {
			s.logger.Error().Err(err).
				Str("mac_address", station.MacAddress).
				Msg("Failed to update station in database after standardization")
			return fmt.Errorf("error updating station in database after standardization: %w", err)
		}

		fmt.Println("LOL")
		return nil
	}

	err := s.SyncToMqtt(ctx, station)
	if err != nil {
		s.logger.Error().Err(err).
			Str("mac_address", station.MacAddress).
			Msg("Failed to sync updated station to MQTTConfig")
		return fmt.Errorf("error syncing updated station to MQTTConfig: %w", err)
	}

	return nil

}

func (s *StationService) GetByMacAddress(ctx context.Context, macAddress string) (*models.Station, error) {
	station, err := s.stationRepository.FindByMacAddress(ctx, macAddress)
	if err != nil {
		return nil, fmt.Errorf("error finding station by MAC address: %w", err)
	}
	return station, nil
}

func (s *StationService) SyncToMqtt(ctx context.Context, station *models.Station) error {
	stationTopic := s.topicManager.GetStationTopic()
	targetTopic := strings.Replace(stationTopic, "+", station.Topic, 1)

	if station.DeletedAt != nil {
		if err := s.client.Publish(targetTopic, nil); err != nil {
			s.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish station deletion to MQTTConfig")
		}
	} else {
		stationDto := station.ToDto()

		if err := s.client.PublishJson(targetTopic, stationDto); err != nil {
			s.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish station data to MQTTConfig")
		}
	}

	err := s.clusterService.SyncAll(ctx)

	if err != nil {
		s.logger.Error().Err(err).
			Msg("Failed to sync clusters after syncing station to MQTTConfig")
	}

	return nil
}

func (s *StationService) SyncAll(ctx context.Context) error {
	stations, err := s.stationRepository.FindAllWhereIsNotDeleted(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to fetch stations from repository")
		return fmt.Errorf("error fetching stations from repository: %w", err)
	}

	for _, station := range stations {
		if err := s.SyncToMqtt(ctx, &station); err != nil {
			s.logger.Error().Err(err).
				Str("mac_address", station.MacAddress).
				Msg("Failed to sync station to MQTTConfig")
			return fmt.Errorf("error syncing station to MQTTConfig: %w", err)
		}
	}

	return nil
}
