package listeners

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
)

type ClusterTableListener struct {
	*BaseTableListener
	logger         zerolog.Logger
	mqttClient     *mq.Client
	topicManager   *mq.TopicManager
	clusterService *services.ClusterService
}

func NewClusterTableListener(
	logger zerolog.Logger,
	mqttClient *mq.Client,
	topicManager *mq.TopicManager,
	clusterService *services.ClusterService,
) *ClusterTableListener {
	return &ClusterTableListener{
		BaseTableListener: NewBaseTableListener("clusters"),
		logger:            logger,
		mqttClient:        mqttClient,
		topicManager:      topicManager,
		clusterService:    clusterService,
	}
}

func (c *ClusterTableListener) HandleChange(ctx context.Context, event *TableChangeEvent) error {
	c.logger.Info().
		Str("operation", string(event.Operation)).
		Str("table", event.Table).
		Time("timestamp", event.Timestamp).
		Msg("Cluster table change detected")

	switch event.Operation {
	case InsertOperation:
		return c.handleInsert(ctx, event)
	case UpdateOperation:
		return c.handleUpdate(ctx, event)
	case DeleteOperation:
		return c.handleDelete(ctx, event)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

func (c *ClusterTableListener) handleInsert(ctx context.Context, event *TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.NewData).
		Msg("New cluster created")

	err := c.clusterService.SyncToMqtt(event)
	if err != nil {
		c.logger.Error().Err(err).
			Msg("Failed to sync cluster to MQTT after creation")
		return err
	}

	topic := c.topicManager.BaseTopic + "/events/clusters/created"
	if err := c.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "cluster_created",
		"cluster":   event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish cluster creation event")
	}

	return nil
}

func (c *ClusterTableListener) handleUpdate(ctx context.Context, event *TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.NewData).
		Msg("Cluster updated")

	err := c.clusterService.SyncToMqtt(event)
	if err != nil {
		c.logger.Error().Err(err).
			Msg("Failed to sync cluster to MQTT after update")
		return err
	}

	topic := c.topicManager.BaseTopic + "/events/stations/updated"
	if err := c.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":     "station_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish station update event")
	}

	return nil
}

func (c *ClusterTableListener) handleDelete(ctx context.Context, event *TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.OldData).
		Msg("Cluster deleted")

	err := c.clusterService.SyncToMqtt(event)
	if err != nil {
		c.logger.Error().Err(err).
			Msg("Failed to sync cluster to MQTT after deletion")
		return err
	}

	topic := c.topicManager.BaseTopic + "/events/clusters/deleted"
	if err := c.mqttClient.PublishJSON(topic, map[string]interface{}{
		"event":        "cluster_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish station deletion event")
	}

	return nil
}
