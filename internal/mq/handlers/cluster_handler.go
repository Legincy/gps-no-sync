package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/services"
	"time"
)

type ClusterHandler struct {
	clusterService *services.ClusterService
	logger         zerolog.Logger
	handlerTopic   string
	topicManager   *mq.TopicManager
}

func NewClusterHandler(
	clusterService *services.ClusterService,
	logger zerolog.Logger,
	topicManager *mq.TopicManager,
) *ClusterHandler {
	return &ClusterHandler{
		clusterService: clusterService,
		logger:         logger,
		handlerTopic:   topicManager.GetClusterTopic(),
		topicManager:   topicManager,
	}
}

func (c *ClusterHandler) TransformMessage(ctx context.Context, msg mqtt.Message) (*mq.ClusterMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message: %w", ErrMessageIsNil)
	}

	topic := msg.Topic()
	payload := string(msg.Payload())

	if len(payload) == 0 {
		return nil, ErrEmptyMessage
	}

	var clusterMessage mq.ClusterMessage
	if err := json.Unmarshal(msg.Payload(), &clusterMessage); err != nil {
		err := c.clusterService.SyncAll(ctx)
		if err != nil {
			c.logger.Error().Err(err).
				Str("topic", topic).
				Msg("Failed to sync all clusters after invalid message")
			return nil, fmt.Errorf("failed to sync clusters: %w", err)
		}
		return nil, fmt.Errorf("could not parse cluster data: %w", ErrInvalidMessage)
	}
	clusterMessage.Topic = c.topicManager.ExtractClusterId(topic)

	return &clusterMessage, nil
}

func (c *ClusterHandler) HandleMessage(client mqtt.Client, msg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := msg.Topic()

	clusterMessage, err := c.TransformMessage(ctx, msg)
	if err != nil {
		if errors.Is(err, ErrEmptyMessage) {
			return
		}

		c.logger.Error().Err(err).
			Str("message", string(msg.Payload())).
			Str("topic", topic).
			Msg("Failed to transform message")
		return
	}

	c.clusterService.ProcessMessage(ctx, clusterMessage)

}
