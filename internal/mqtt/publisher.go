package mqtt

import (
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	"time"
)

type Publisher interface {
	PublishInterface(topic string, message interface{}, qos byte, retained bool) error
}

type PublisherImpl struct {
	client mqtt.Client
	logger zerolog.Logger
}

func NewPublisher(client mqtt.Client, logger zerolog.Logger) *PublisherImpl {
	return &PublisherImpl{
		client: client,
		logger: logger.With().Str("component", "publisher").Logger(),
	}
}

func (p *PublisherImpl) PublishInterface(topic string, message interface{}, qos byte, retained bool) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	token := p.client.Publish(topic, qos, retained, payload)
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("publish timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	p.logger.Debug().
		Str("topic", topic).
		Int("payload_size", len(payload)).
		Msg("Message published")

	return nil
}

func (p *PublisherImpl) Publish(topic string, payload []byte, qos byte, retained bool) error {
	token := p.client.Publish(topic, qos, retained, payload)

	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("publish timeout")
	}

	if err := token.Error(); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	p.logger.Debug().
		Str("topic", topic).
		Int("size", len(payload)).
		Bool("retained", retained).
		Msg("Raw message published")

	return nil
}

func (p *PublisherImpl) ClearRetainedMessage(topic string) error {
	return p.Publish(topic, nil, 0, true)
}

func (p *PublisherImpl) PublishStation(station *models.Station) error {
	stationDto := station.ToDto()
	msg := OutgoingMessage{
		Source:    "SYNC",
		Data:      stationDto,
		Timestamp: time.Now(),
	}

	topic := fmt.Sprintf("gpsno/v2/stations/%s", station.Topic)

	return p.PublishInterface(topic, msg, 1, true)
}

func (p *PublisherImpl) PublishStationList(stations []*models.Station) error {
	for _, station := range stations {
		err := p.PublishStation(station)
		if err != nil {
			return fmt.Errorf("failed to publish station: %w", err)
		}
	}

	return nil
}
