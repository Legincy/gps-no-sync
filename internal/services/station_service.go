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

	fmt.Printf("%+v\n", *station.ClusterId)

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

	fmt.Printf("Processing station: %+v\n", station)
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
	if station.Topic == "" {
		return fmt.Errorf("topic_id is required")
	}
	return nil
}

func (s *StationService) SyncToMqtt(station *models.Station) error {
	topic := s.topicManager.GetStationTopic()
	topic = strings.Replace(topic, "+", strings.ToLower(station.Topic), 1)
	stationDto := station.ToMqDto()

	if err := s.client.PublishJSON(topic, stationDto); err != nil {
		return err
	}

	return nil
}
