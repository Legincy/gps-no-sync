package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"strings"
)

type StationService struct {
	stationRepository *repositories.StationRepository
	clusterRepository *repositories.ClusterRepository
	clusterService    *ClusterService
	client            *mq.Client
	topicManager      interfaces.ITopicManager
	logger            zerolog.Logger
}

func NewStationService(stationRepository *repositories.StationRepository, clusterService *ClusterService, clusterRepository *repositories.ClusterRepository, client *mq.Client, topicManager interfaces.ITopicManager, logger zerolog.Logger) *StationService {
	return &StationService{
		stationRepository: stationRepository,
		clusterRepository: clusterRepository,
		clusterService:    clusterService,
		client:            client,
		topicManager:      topicManager,
		logger:            logger,
	}
}

func (s *StationService) ProcessStation(ctx context.Context, station *models.Station) error {
	if station.ClusterID != nil && *station.ClusterID > 0 {
		dbCluster, err := s.clusterRepository.FindById(ctx, *station.ClusterID)
		if err != nil {
			s.logger.Error().Err(err).
				Str("cluster_id", fmt.Sprintf("%d", *station.ClusterID)).
				Msg("Failed to find cluster for station")

			station.ClusterID = nil
			station.Cluster = nil
		} else {
			station.ClusterID = &dbCluster.ID
			station.Cluster = dbCluster
		}
	} else {
		station.ClusterID = nil
		station.Cluster = nil
	}

	if err := s.stationRepository.CreateOrUpdate(ctx, station); err != nil {
		return fmt.Errorf("error saving device to database: %w", err)
	}

	s.logger.Info().
		Str("mac_address", station.MacAddress).
		Msg("Station data processed and saved")

	return nil
}

func (s *StationService) FindByMacAddress(ctx context.Context, macAddress string) (*models.Station, error) {
	station, err := s.stationRepository.FindByMacAddress(ctx, macAddress)
	if err != nil {
		return nil, fmt.Errorf("error finding station by MAC address: %w", err)
	}
	return station, nil
}

func (s *StationService) SyncToMqtt(ctx context.Context, station *models.Station) error {
	stationTopic := s.topicManager.GetStationTopic()
	targetTopic := strings.Replace(stationTopic, "+", station.Topic, 1)
	stationDto := station.ToMqDto()

	if !station.DeletedAt.IsZero() {
		if err := s.client.Publish(targetTopic, nil); err != nil {
			s.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish station deletion to MQTT")
		}
	} else {
		if err := s.client.PublishJson(targetTopic, stationDto); err != nil {
			s.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish station data to MQTT")
		}
	}

	err := s.clusterService.SyncAll(ctx)
	if err != nil {
		s.logger.Error().Err(err).
			Msg("Failed to sync clusters after syncing station to MQTT")
	}

	return nil
}
