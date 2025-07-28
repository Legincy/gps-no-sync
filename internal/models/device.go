package models

import (
	"gorm.io/gorm"
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

func (d *Device) clusterIDEquals(other *uint) bool {
	if d.ClusterID == nil && other == nil {
		return true
	}
	if d.ClusterID == nil || other == nil {
		return false
	}
	return *d.ClusterID == *other
}
