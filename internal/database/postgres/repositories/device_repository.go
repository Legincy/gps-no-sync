package repositories

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"gps-no-sync/internal/models"
	"strings"
	"time"
)

type DeviceRepository struct {
	db *gorm.DB
}

func NewDeviceRepository(db *gorm.DB) *DeviceRepository {
	return &DeviceRepository{db: db}
}

func (r *DeviceRepository) CreateOrUpdate(ctx context.Context, device *models.Device) error {
	now := time.Now()
	device.LastSeen = now

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existingDevice models.Device
		err := tx.Where("mac_address = ?", device.MacAddress).First(&existingDevice).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			if device.Name == "" {
				macSplit := strings.Split(device.MacAddress, ":")
				macIdentifier := strings.Join(macSplit[3:5], "")
				device.Name = "Device_" + macIdentifier
			}
			return tx.Create(device).Error
		} else if err != nil {
			return err
		}

		existingDevice.Name = device.Name
		existingDevice.DeviceType = device.DeviceType
		existingDevice.LastSeen = now

		return tx.Save(&existingDevice).Error
	})
}

func (r *DeviceRepository) FindByMacAddress(ctx context.Context, macAddress string) (*models.Device, error) {
	var device models.Device
	err := r.db.WithContext(ctx).Preload("Cluster").Where("mac_address = ?", macAddress).First(&device).Error
	if err != nil {
		return nil, err
	}
	return &device, nil
}

func (r *DeviceRepository) UpdateLastSeen(ctx context.Context, macAddress string) error {
	return r.db.WithContext(ctx).Model(&models.Device{}).
		Where("macAddress  = ?", macAddress).
		Update("last_seen", time.Now()).Error
}

func (r *DeviceRepository) MarkInactiveDevices(ctx context.Context, timeout time.Duration) error {
	cutoff := time.Now().Add(-timeout)
	return r.db.WithContext(ctx).Model(&models.Device{}).
		Where("last_seen < ? AND is_active = ?", cutoff, true).
		Update("is_active", false).Error
}

func (r *DeviceRepository) GetAllDevices(ctx context.Context) ([]*models.Device, error) {
	var devices []*models.Device
	err := r.db.WithContext(ctx).Find(&devices).Error
	return devices, err
}
