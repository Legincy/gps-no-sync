package services

import (
	"context"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"strconv"
	"strings"
)

type ClusterService struct {
	clusterRepository *repositories.ClusterRepository
	client            *mq.Client
	topicManager      *mq.TopicManager
	logger            zerolog.Logger
}

func NewClusterService(clusterRepository *repositories.ClusterRepository, client *mq.Client, topicManager *mq.TopicManager, logger zerolog.Logger) *ClusterService {
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

	if cluster.DeletedAt != nil {
		if err := c.client.Publish(targetTopic, nil); err != nil {
			c.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish cluster deletion to MQTT")
		}
	} else {
		clusterDto := cluster.ToDto()

		if err := c.client.PublishJson(targetTopic, clusterDto); err != nil {
			c.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish cluster data to MQTT")
		}
	}

	return nil
}

func (c *ClusterService) SyncAll(ctx context.Context) error {
	cluster, err := c.clusterRepository.FindAllWhereStationDeletedAtIsNull(ctx)
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

		c.logger.Debug().
			Int("cluster_id", int(cl.ID)).
			Msg("Cluster synced to MQTT successfully")
	}

	return nil
}

func (c *ClusterService) ProcessMessage(ctx context.Context, clusterMessage *mq.ClusterMessage) {
	if clusterMessage.Source == "SYNC" {
		return
	}

	clusterDto := clusterMessage.Data

	dbCluster, _ := c.clusterRepository.FindByName(ctx, clusterDto.Name)
	if dbCluster != nil {
		err := c.SyncToMqtt(ctx, dbCluster)
		if err != nil {
			c.logger.Error().Err(err).
				Str("cluster_name", clusterDto.Name).
				Msg("Failed to sync existing cluster to MQTT")
			return
		}
	} else {
		clusterTopic := c.topicManager.GetClusterTopic()
		targetTopic := strings.Replace(clusterTopic, "+", clusterMessage.Topic, 1)

		if err := c.client.Publish(targetTopic, nil); err != nil {
			c.logger.Error().Err(err).
				Str("topic", targetTopic).
				Msg("Failed to publish")
		}
	}
}

func (c *ClusterService) ProcessDbCreate(ctx context.Context, cluster *models.Cluster) error {
	if cluster == nil {
		return nil
	}

	c.logger.Debug().
		Int("cluster_id", int(cluster.ID)).
		Msg("Processing cluster creation to MQTT")

	if err := c.SyncToMqtt(ctx, cluster); err != nil {
		c.logger.Error().Err(err).
			Int("cluster_id", int(cluster.ID)).
			Msg("Failed to process cluster creation to MQTT")
		return err
	}

	return nil
}

func (c *ClusterService) ProcessDbUpdate(ctx context.Context, cluster *models.Cluster) error {
	if cluster == nil {
		return nil
	}

	c.logger.Debug().
		Int("cluster_id", int(cluster.ID)).
		Msg("Processing cluster update to MQTT")

	if err := c.SyncToMqtt(ctx, cluster); err != nil {
		c.logger.Error().Err(err).
			Int("cluster_id", int(cluster.ID)).
			Msg("Failed to process cluster update to MQTT")
		return err
	}

	return nil
}

func (c *ClusterService) ProcessDbDelete(ctx context.Context, cluster *models.Cluster) error {
	if cluster == nil {
		return nil
	}

	c.logger.Debug().
		Int("cluster_id", int(cluster.ID)).
		Msg("Processing cluster deletion to MQTT")

	clusterTopic := c.topicManager.GetClusterTopic()
	targetTopic := strings.Replace(clusterTopic, "+", strconv.Itoa(int(cluster.ID)), 1)

	if err := c.client.Publish(targetTopic, nil); err != nil {
		c.logger.Error().Err(err).
			Str("topic", targetTopic).
			Msg("Failed to publish cluster deletion to MQTT")
		return err
	}

	return nil
}
