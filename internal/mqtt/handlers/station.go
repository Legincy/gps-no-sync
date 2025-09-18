package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt2 "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/services"
	"strings"
)

type IncomingStationMessage struct {
	Action mqtt.Action            `json:"action"`
	Source string                 `json:"source"`
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
}

func NewStationHandler(
	stationService *services.StationService,
	publisher *mqtt.PublisherImpl,
	logger zerolog.Logger,
) *StationHandlerImpl {
	return &StationHandlerImpl{
		stationService: stationService,
		publisher:      publisher,
		logger:         logger.With().Str("handler", "station").Logger(),
	}
}

func (h *StationHandlerImpl) Process(ctx context.Context, msg mqtt2.Message) error {
	_ = msg.Topic()
	payload := msg.Payload()

	var stationMessage IncomingStationMessage
	if err := json.Unmarshal(payload, &stationMessage); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	switch stationMessage.Action {
	case mqtt.ActionRegister:
		return h.HandleRegister(ctx, &stationMessage)
	default:
		return fmt.Errorf("unknown action: %s", stationMessage.Action)
	}
}

func (h *StationHandlerImpl) HandleRegister(ctx context.Context, msg *IncomingStationMessage) error {
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

func (m *IncomingStationMessage) ToStationDto() (*models.StationDto, error) {
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
