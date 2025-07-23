package models

import (
	"gorm.io/gorm"
	"gps-no-sync/internal/database/postgres/events"
	"time"
)

type DeviceType string

const (
	DeviceTypeAnchor  DeviceType = "ANCHOR"
	DeviceTypeTag     DeviceType = "TAG"
	DeviceTypeUnknown DeviceType = "UNKNOWN"
)

type Device struct {
	gorm.Model
	MacAddress string     `gorm:"uniqueIndex;not null" json:"mac_address"`
	Name       string     `gorm:"not null" json:"name"`
	DeviceType DeviceType `gorm:"type:varchar(10);not null" json:"type"`
	ClusterID  *uint      `json:"cluster_id,omitempty"`
	Cluster    *Cluster   `gorm:"foreignKey:ClusterID" json:"cluster,omitempty"`
	LastSeen   time.Time  `gorm:"not null;default:CURRENT_TIMESTAMP" json:"last_seen"`

	previousValues *events.DeviceSnapshot `gorm:"-" json:"-"`
}

type DeviceRawData struct {
	UWB struct {
		DeviceType string      `json:"device_type"`
		Cluster    interface{} `json:"cluster"`
	} `json:"uwb"`
	Device struct {
		MacAddress string `json:"mac_address"`
		Name       string `json:"name"`
	} `json:"device"`
}

func (d *Device) AfterFind(tx *gorm.DB) error {
	d.previousValues = d.ToSnapshot()
	return nil
}

func (d *Device) AfterCreate(tx *gorm.DB) error {
	if events.DeviceEventPublisher != nil {
		event := &events.DeviceChangeEvent{
			Action:     "created",
			DeviceID:   d.ID,
			MacAddress: d.MacAddress,
			NewValues:  d.ToSnapshot(),
			ChangedAt:  time.Now(),
		}

		go events.DeviceEventPublisher(event)
	}
	return nil
}

func (d *Device) AfterUpdate(tx *gorm.DB) error {
	if events.DeviceEventPublisher != nil {
		changedFields := d.getChangedFields()

		event := &events.DeviceChangeEvent{
			Action:        "updated",
			DeviceID:      d.ID,
			MacAddress:    d.MacAddress,
			OldValues:     d.previousValues,
			NewValues:     d.ToSnapshot(),
			ChangedFields: changedFields,
			ChangedAt:     time.Now(),
		}

		go events.DeviceEventPublisher(event)
	}
	return nil
}

func (d *Device) AfterDelete(tx *gorm.DB) error {
	if events.DeviceEventPublisher != nil {
		event := &events.DeviceChangeEvent{
			Action:     "deleted",
			DeviceID:   d.ID,
			MacAddress: d.MacAddress,
			OldValues:  d.ToSnapshot(),
			ChangedAt:  time.Now(),
		}

		go events.DeviceEventPublisher(event)
	}
	return nil
}

func (d *Device) getChangedFields() []string {
	if d.previousValues == nil {
		return []string{} // Alle Felder sind "neu"
	}

	var changed []string

	if d.Name != d.previousValues.Name {
		changed = append(changed, "name")
	}
	if d.DeviceType != d.previousValues.DeviceType {
		changed = append(changed, "device_type")
	}
	/*
		if !d.clusterIDEquals(d.previousValues.ClusterID) {
			changed = append(changed, "cluster_id")
		}
	*/

	return changed
}

func (d *Device) clusterIDEquals(other *uint) bool {
	if d.ClusterID == nil && other == nil {
		return true
	}
	if d.ClusterID == nil || other == nil {
		return false
	}
	return *d.ClusterID == *other
}

func (d *Device) ToSnapshot() *events.DeviceSnapshot {
	return &events.DeviceSnapshot{
		ID:         d.ID,
		MacAddress: d.MacAddress,
		Name:       d.Name,
		DeviceType: d.DeviceType,
		ClusterID:  d.ClusterID,
		LastSeen:   d.LastSeen,
		CreatedAt:  d.CreatedAt,
		UpdatedAt:  d.UpdatedAt,
	}
}
