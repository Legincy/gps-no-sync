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
	client            *mq.Client
	topicManager      *mq.TopicManager
	logger            zerolog.Logger
}

func NewDeviceService(stationRepository *repositories.StationRepository, clusterRepository *repositories.ClusterRepository, client *mq.Client, topicManager *mq.TopicManager, logger zerolog.Logger) *StationService {
	return &StationService{
		stationRepository: stationRepository,
		clusterRepository: clusterRepository,
		client:            client,
		topicManager:      topicManager,
		logger:            logger,
	}
}

func (s *StationService) ProcessStation(ctx context.Context, station *models.Station) error {
	err := s.validate(station)
	if err != nil {
		s.logger.Error().Err(err).
			Msg("Invalid station data received")
	}

	dbCluster, err := s.clusterRepository.FindById(ctx, *station.ClusterId)
	if err != nil {
		s.logger.Error().Err(err).
			Str("cluster_id", fmt.Sprintf("%d", *station.ClusterId)).
			Msg("Failed to find cluster for station")

		station.ClusterId = nil
		station.Cluster = nil
	} else {
		station.ClusterId = &dbCluster.ID
		station.Cluster = dbCluster
	}

	if err := s.stationRepository.CreateOrUpdate(ctx, station); err != nil {
		return fmt.Errorf("error saving device to database: %w", err)
	}

	s.logger.Info().
		Str("mac_address", station.MacAddress).
		Msg("Station data processed and saved")

	return nil
}

func (s *StationService) validate(station *models.Station) error {
	if station.MacAddress == "" {
		return fmt.Errorf("mac_address is required")
	}
	if station.TopicId == "" {
		return fmt.Errorf("topic_id is required")
	}
	return nil
}

func (s *StationService) SyncToMqtt(device *models.Station) error {
	topic := s.topicManager.GetStationTopic()
	address := strings.ReplaceAll(device.MacAddress, ":", "")
	topic = strings.Replace(topic, "+", strings.ToLower(address), 1)

	if err := s.client.PublishJSON(topic, device); err != nil {
		return err
	}

	return nil
}
