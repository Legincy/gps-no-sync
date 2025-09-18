package repositories

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"gps-no-sync/internal/models"
)

type StationRepository struct {
	db *gorm.DB
}

func NewStationRepository(db *gorm.DB) *StationRepository {
	return &StationRepository{db: db}
}

func (r *StationRepository) CreateOrUpdate(ctx context.Context, station *models.Station) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existingStation models.Station
		result := tx.Where("mac_address = ?", station.MacAddress).First(&existingStation)

		if result.Error == nil {
			updateMap := map[string]interface{}{
				"name":   station.Name,
				"topic":  station.Topic,
				"config": station.Config,
			}

			return tx.Model(&models.Station{}).
				Where("mac_address = ?", station.MacAddress).
				Updates(updateMap).Error

		} else if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return tx.Create(station).Error

		} else {
			return result.Error
		}
	})
}

func (r *StationRepository) Create(ctx context.Context, station *models.Station) error {
	return r.db.WithContext(ctx).Create(station).Error
}

func (r *StationRepository) Update(ctx context.Context, station *models.Station) error {
	return r.db.WithContext(ctx).Model(&models.Station{}).
		Where("LOWER(mac_address) = ?", station.MacAddress).
		Updates(map[string]interface{}{
			"name":        station.Name,
			"topic":       station.Topic,
			"config":      station.Config,
			"mac_address": station.MacAddress,
		}).Error
}

func (r *StationRepository) FindByMacAddress(ctx context.Context, macAddress string) (*models.Station, error) {
	var device models.Station
	err := r.db.WithContext(ctx).Preload("Cluster").Where("mac_address = ?", macAddress).First(&device).Error
	if err != nil {
		return nil, err
	}
	return &device, nil
}

func (r *StationRepository) FindAllWhereIsNotDeleted(ctx context.Context) ([]models.Station, error) {
	var stations []models.Station
	err := r.db.WithContext(ctx).Preload("Cluster").Where("deleted_at IS NULL").Find(&stations).Error
	if err != nil {
		return nil, err
	}
	return stations, nil
}

func (r *StationRepository) FindAll(ctx context.Context) ([]*models.Station, error) {
	var stations []*models.Station
	err := r.db.WithContext(ctx).Find(&stations).Error
	if err != nil {
		return nil, err
	}
	return stations, nil
}
