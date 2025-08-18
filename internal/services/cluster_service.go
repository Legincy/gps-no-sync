package services

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/listeners"
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

func (c *ClusterService) SyncToMqtt(event *listeners.TableChangeEvent) error {
	clusterTopic := c.topicManager.GetClusterTopic()
	newData, err := json.Marshal(event.NewData)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to marshal new data")

		return err
	}

	var cluster models.Cluster
	if err := json.Unmarshal(newData, &clusterTopic); err != nil {
		c.logger.Error().Err(err).
			Msg("Could not parse clusterTopic data")
		return err
	}

	targetTopic := strings.Replace(clusterTopic, "+", strconv.Itoa(int(clusterTopic.ID)), 1)
	clusterDto := cluster.ToMqDto()

	switch event.Operation {
	case listeners.InsertOperation:
		fmt.Printf("Insert")
	case listeners.UpdateOperation:
		fmt.Printf("Update")
	case listeners.DeleteOperation:
		fmt.Printf("Delete")
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}

	if err := c.client.PublishJSON(targetTopic, clusterDto); err != nil {
		return err
	}

	return nil
}
