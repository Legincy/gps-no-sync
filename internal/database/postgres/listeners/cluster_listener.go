package listeners

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
	"time"
)

type ClusterTableListener struct {
	*BaseTableListener
	logger         zerolog.Logger
	mqttClient     *mq.Client
	topicManager   *mq.TopicManager
	clusterService services.ClusterService
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
		clusterService:    *clusterService,
	}
}

func (c *ClusterTableListener) HandleChange(ctx context.Context, event *interfaces.TableChangeEvent) error {
	c.logger.Info().
		Str("operation", string(event.Operation)).
		Str("table", event.Table).
		Time("timestamp", event.Timestamp).
		Msg("Cluster table change detected")

	switch event.Operation {
	case interfaces.InsertOperation:
		return c.handleInsert(ctx, event)
	case interfaces.UpdateOperation:
		return c.handleUpdate(ctx, event)
	case interfaces.DeleteOperation:
		return c.handleDelete(ctx, event)
	default:
		return fmt.Errorf("unknown operation: %s", event.Operation)
	}
}

func (c *ClusterTableListener) handleInsert(ctx context.Context, event *interfaces.TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.NewData).
		Msg("New cluster created")

	newData, _, err := event.GetData()
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	cluster := &models.Cluster{}
	err = json.Unmarshal(newData, &cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to marshal cluster data")
	}

	err = c.clusterService.ProcessDbCreate(ctx, cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to process cluster creation")
	}

	baseTopic := c.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/clusters/created"
	if err := c.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":     "cluster_created",
		"cluster":   event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish cluster creation event")
	}

	return nil
}

func (c *ClusterTableListener) handleUpdate(ctx context.Context, event *interfaces.TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.NewData).
		Msg("Cluster updated")

	newData, _, err := event.GetData()
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	cluster := &models.Cluster{}
	err = json.Unmarshal(newData, &cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to marshal cluster data")
	}

	err = c.clusterService.ProcessDbUpdate(ctx, cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to process cluster update")
	}

	baseTopic := c.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/clusters/updated"
	if err := c.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":     "cluster_updated",
		"old_data":  event.OldData,
		"new_data":  event.NewData,
		"timestamp": event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish cluster update event")
	}

	return nil
}

func (c *ClusterTableListener) handleDelete(ctx context.Context, event *interfaces.TableChangeEvent) error {
	c.logger.Info().
		Interface("cluster_data", event.OldData).
		Msg("Cluster deleted")

	_, oldData, err := event.GetData()
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to get data from event")
	}

	cluster := &models.Cluster{}
	err = json.Unmarshal(oldData, &cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to marshal cluster data")
	}
	now := time.Now()
	cluster.DeletedAt = &now

	err = c.clusterService.ProcessDbDelete(ctx, cluster)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to process cluster deletion")
		return err
	}

	baseTopic := c.topicManager.GetBaseTopic()
	topic := baseTopic + "/events/clusters/deleted"
	if err := c.mqttClient.PublishJson(topic, map[string]interface{}{
		"event":        "cluster_deleted",
		"deleted_data": event.OldData,
		"timestamp":    event.Timestamp,
	}); err != nil {
		c.logger.Error().Err(err).Msg("Failed to publish cluster deletion event")
	}

	return nil
}
