package services

import (
	"context"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"strconv"
	"strings"
)

type ClusterService struct {
	clusterRepository *repositories.ClusterRepository
	client            *mq.Client
	topicManager      interfaces.ITopicManager
	logger            zerolog.Logger
}

func NewClusterService(clusterRepository *repositories.ClusterRepository, client *mq.Client, topicManager interfaces.ITopicManager, logger zerolog.Logger) *ClusterService {
	return &ClusterService{
		clusterRepository: clusterRepository,
		client:            client,
		topicManager:      topicManager,
		logger:            logger,
	}
}

func (c *ClusterService) SyncToMqtt(ctx context.Context, cluster *models.Cluster) error {
	clusterTopic := c.topicManager.GetClusterTopic()
	targetTopic := strings.Replace(clusterTopic, "+", strconv.Itoa(int(cluster.ID)), 1)
	clusterDto := cluster.ToMqDto()

	if err := c.client.PublishJson(targetTopic, clusterDto); err != nil {
		return err
	}

	return nil
}

func (c *ClusterService) RemoveFromMqtt(ctx context.Context, cluster *models.Cluster) error {
	clusterTopic := c.topicManager.GetClusterTopic()
	targetTopic := strings.Replace(clusterTopic, "+", strconv.Itoa(int(cluster.ID)), 1)

	if err := c.client.Publish(targetTopic, nil); err != nil {
		return err
	}

	return nil
}

func (c *ClusterService) SyncAll(ctx context.Context) error {
	cluster, err := c.clusterRepository.GetAllWhereStationDeletedAtIsNull(ctx)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to fetch clusters from repository")
	}

	for _, cl := range cluster {
		if err := c.SyncToMqtt(ctx, &cl); err != nil {
			c.logger.Error().Err(err).
				Int("cluster_id", int(cl.ID)).
				Msg("Failed to sync cluster to MQTT")
			return err
		}

		c.logger.Info().
			Int("cluster_id", int(cl.ID)).
			Msg("Cluster synced to MQTT successfully")
	}

	return nil
}
