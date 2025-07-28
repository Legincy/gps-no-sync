package repositories

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gps-no-sync/internal/models"
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
		result := tx.Where("mac_address = ?", device.MacAddress).First(&existingDevice)

		if result.Error == nil {
			fmt.Printf("Updating device: %s\n", device.MacAddress)

			return tx.Model(&existingDevice).Updates(map[string]interface{}{
				"name":        device.Name,
				"device_type": device.DeviceType,
				"last_seen":   now,
			}).Error

		} else if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			fmt.Printf("Creating device: %s\n", device.MacAddress)

			/*
				if device.Name == "" {
					macSplit := strings.Split(device.MacAddress, ":")
					if len(macSplit) >= 5 {
						macIdentifier := strings.Join(macSplit[3:6], "")
						device.Name = "Device_" + macIdentifier
					}
				}
			*/

			return tx.Create(device).Error

		} else {
			return result.Error
		}
	})
}

func (r *DeviceRepository) FindByMacAddress(ctx context.Context, macAddress string) (*models.Device, error) {
	var device models.Device
	fmt.Printf("FindByMacAddress: %+v\n", macAddress)
	err := r.db.WithContext(ctx).Preload("Cluster").Where("mac_address = ?", macAddress).First(&device).Error
	if err != nil {
		return nil, err
	}
	fmt.Printf("Found device: %+v\n", device)
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
