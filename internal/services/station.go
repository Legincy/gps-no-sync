package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/models"
)

type StationService struct {
	stationRepository *repositories.StationRepository
	logger            zerolog.Logger
}

func NewStationService(stationRepository *repositories.StationRepository, logger zerolog.Logger) *StationService {
	return &StationService{
		stationRepository: stationRepository,
		logger:            logger,
	}
}

func (s *StationService) IsValidStation(station *models.Station) bool {
	if station == nil {
		return false
	}

	if station.MacAddress == "" {
		return false
	}

	return true
}

func (s *StationService) GetByMacAddress(ctx context.Context, macAddress string) (*models.Station, error) {
	station, err := s.stationRepository.FindByMacAddress(ctx, macAddress)
	if err != nil {
		return nil, fmt.Errorf("error finding station by MAC address: %w", err)
	}
	return station, nil
}

func (s *StationService) GetAll(ctx context.Context) ([]*models.Station, error) {
	stations, err := s.stationRepository.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("error retrieving stations: %w", err)
	}
	return stations, nil
}

func (s *StationService) RegisterStation(ctx context.Context, station *models.Station) error {
	if station.IsValid() == false {
		return fmt.Errorf("invalid station data")
	}

	//TODO Check if station with same MAC address already exists and if topic is not already in use

	err := s.stationRepository.Create(ctx, station)
	if err != nil {
		return fmt.Errorf("error creating station: %w", err)
	}

	return nil
}
