package mqtt

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

type SubscriberImpl struct {
	client mqtt.Client
	router *RouterImpl
	logger zerolog.Logger

	subscriptions map[string]byte
	mu            sync.RWMutex
}

func NewSubscriber(client mqtt.Client, router *RouterImpl, logger zerolog.Logger) *SubscriberImpl {
	return &SubscriberImpl{
		client:        client,
		router:        router,
		logger:        logger.With().Str("component", "subscriber").Logger(),
		subscriptions: make(map[string]byte),
	}
}

func (s *SubscriberImpl) SubscribeMultiple(topics []string, qos byte) error {
	for _, topic := range topics {
		if err := s.Subscribe(topic, qos); err != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}
	}
	return nil
}

func (s *SubscriberImpl) Subscribe(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := s.client.Subscribe(topic, qos, s.messageHandler)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("subscribe timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	s.subscriptions[topic] = qos
	s.logger.Info().Str("topic", topic).Msg("Subscribed to topic")

	return nil
}

func (s *SubscriberImpl) messageHandler(client mqtt.Client, mqttMsg mqtt.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := mqttMsg.Topic()
	payload := mqttMsg.Payload()

	s.logger.Debug().
		Str("topic", topic).
		Int("size", len(payload)).
		Msg("Message received")

	if err := s.router.Route(ctx, topic, mqttMsg); err != nil {
		s.logger.Error().Err(err).
			Str("topic", topic).
			Msg("Handler failed")
	}
}
