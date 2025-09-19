package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	mq "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
	"strings"
)

type StationRootMessage struct {
	Action mqtt.Action            `json:"action"`
	Source string                 `json:"source"`
	Data   map[string]interface{} `json:"data"`
}

type IncomingStationSubMessage struct {
	string `json:"source"`
	Data   map[string]interface{} `json:"data"`
}

type StationHandler interface {
	mqtt.TopicHandler
	HandleRegister(ctx context.Context, msg *mqtt.IncomingMessage) error
}

type StationHandlerImpl struct {
	stationService *services.StationService
	publisher      *mqtt.PublisherImpl
	logger         zerolog.Logger
	topicManager   *mqtt.TopicManagerImpl
}

func NewStationHandler(
	stationService *services.StationService,
	publisher *mqtt.PublisherImpl,
	topicManager *mqtt.TopicManagerImpl,
	logger zerolog.Logger,
) *StationHandlerImpl {
	return &StationHandlerImpl{
		stationService: stationService,
		publisher:      publisher,
		logger:         logger.With().Str("handler", "station").Logger(),
		topicManager:   topicManager,
	}
}

func (h *StationHandlerImpl) Process(ctx context.Context, msg mq.Message) error {
	topic := msg.Topic()
	payload := msg.Payload()

	topicInfo, err := h.topicManager.ParseTopic(topic)
	if err != nil {
		return fmt.Errorf("invalid topic: %w", err)
	}

	h.logger.Debug().
		Str("topic", topic).
		Str("type", topicInfo.Type).
		Str("id", topicInfo.ID).
		Bool("is_root", topicInfo.IsRoot).
		Msg("Processing station message")

	if topicInfo.IsRoot {
		var stationMessage StationRootMessage
		if err := json.Unmarshal(payload, &stationMessage); err != nil {
			return fmt.Errorf("failed to parse message: %w", err)
		}

		switch stationMessage.Action {
		case mqtt.ActionRegister:
			return h.HandleRegister(ctx, &stationMessage)
		default:
			return fmt.Errorf("unknown action: %s", stationMessage.Action)
		}
	} else {
		if len(payload) == 0 {
			if err := h.publisher.ClearRetainedMessage(topic); err != nil {
				return fmt.Errorf("failed to publish empty message to unregistered station: %w", err)
			}
		}
	}

	return nil
}

func (h *StationHandlerImpl) HandleSubMessage(ctx context.Context, msg mq.Message) error {
	topic := msg.Topic()

	station, err := h.stationService.GetByTopic(ctx, topic)
	fmt.Println(station, err, topic, string(msg.Payload()))
	if err != nil {
		if len(string(msg.Payload())) > 0 {
			if err := h.publisher.ClearRetainedMessage(topic); err != nil {
				return fmt.Errorf("failed to publish empty message to unregistered station: %w", err)
			}
		}
		return nil
	}

	if err := h.publisher.PublishStation(station); err != nil {
		return fmt.Errorf("failed to republish station entity: %w", err)
	}

	return nil
}

func (h *StationHandlerImpl) HandleRegister(ctx context.Context, msg *StationRootMessage) error {
	h.logger.Info().
		Str("source", msg.Source).
		Str("action", string(msg.Action)).
		Msg("Processing station registration")

	jsonData, err := json.Marshal(msg.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	var stationRegisterDto models.StationRegisterDto
	if err := json.Unmarshal(jsonData, &stationRegisterDto); err != nil {
		return fmt.Errorf("failed to unmarshal to StationDTO: %w", err)
	}
	stationRegisterDto.MacAddress = normalizeMacAddress(msg.Source)

	station := stationRegisterDto.ToStation()
	station.Prepare()

	err = h.stationService.RegisterStation(ctx, station)
	if err != nil {
		h.logger.Error().Err(err).Str("mac", msg.Source).Msg("Failed to register station")
		return fmt.Errorf("failed to register station: %w", err)
	}

	err = h.publisher.PublishStation(station)
	if err != nil {
		h.logger.Error().Err(err).
			Str("topic", station.Topic).
			Msg("Failed to publish station entity")
		return err
	}

	h.logger.Info().
		Str("mac", station.MacAddress).
		Str("topic", station.Topic).
		Uint("id", station.ID).
		Msg("Station registered successfully")

	return nil
}

func (m *StationRootMessage) ToStationDto() (*models.StationDto, error) {
	var stationDto models.StationDto

	stationDto.MacAddress = normalizeMacAddress(m.Source)

	jsonData, err := json.Marshal(m.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	if err := json.Unmarshal(jsonData, &stationDto); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to StationDTO: %w", err)
	}

	return &stationDto, nil
}

func normalizeMacAddress(mac string) string {
	mac = strings.ToLower(mac)

	if len(mac) == 12 && !strings.Contains(mac, ":") {
		parts := []string{
			mac[0:2], mac[2:4], mac[4:6],
			mac[6:8], mac[8:10], mac[10:12],
		}
		return strings.Join(parts, ":")
	}

	return mac
}
