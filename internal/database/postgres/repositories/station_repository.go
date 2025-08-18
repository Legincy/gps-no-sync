package repositories

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"gps-no-sync/internal/models"
	"time"
)

type StationRepository struct {
	db *gorm.DB
}

func NewStationRepository(db *gorm.DB) *StationRepository {
	return &StationRepository{db: db}
}

func (r *StationRepository) CreateOrUpdate(ctx context.Context, device *models.Station) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existingStation models.Station
		result := tx.Where("mac_address = ?", device.MacAddress).First(&existingStation)

		if result.Error == nil {

			updateMap := map[string]interface{}{
				"name":       device.Name,
				"topic":      device.Topic,
				"config":     device.Config,
				"cluster_id": device.ClusterID,
			}

			return tx.Model(&models.Station{}).
				Where("mac_address = ?", device.MacAddress).
				Updates(updateMap).Error

		} else if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return tx.Create(device).Error

		} else {
			return result.Error
		}
	})
}

func (r *StationRepository) FindByMacAddress(ctx context.Context, macAddress string) (*models.Station, error) {
	var device models.Station
	err := r.db.WithContext(ctx).Preload("Cluster").Where("mac_address = ?", macAddress).First(&device).Error
	if err != nil {
		return nil, err
	}
	return &device, nil
}

func (r *StationRepository) UpdateLastSeen(ctx context.Context, macAddress string) error {
	return r.db.WithContext(ctx).Model(&models.Station{}).
		Where("macAddress  = ?", macAddress).
		Update("last_seen", time.Now()).Error
}

func (r *StationRepository) MarkInactiveDevices(ctx context.Context, timeout time.Duration) error {
	cutoff := time.Now().Add(-timeout)
	return r.db.WithContext(ctx).Model(&models.Station{}).
		Where("last_seen < ? AND is_active = ?", cutoff, true).
		Update("is_active", false).Error
}

func (r *StationRepository) GetAllDevices(ctx context.Context) ([]*models.Station, error) {
	var devices []*models.Station
	err := r.db.WithContext(ctx).Find(&devices).Error
	return devices, err
}
