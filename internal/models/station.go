package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
)

type DW3000Mode string

const (
	DW3000ModeAnchor  DW3000Mode = "ANCHOR"
	DW3000ModeTag     DW3000Mode = "TAG"
	DW3000ModeUnknown DW3000Mode = "UNKNOWN"
)

type StationConfig struct {
	UWB *UWBConfig `json:"uwb,omitempty"`
}

type UWBConfig struct {
	Mode DW3000Mode `json:"mode"`
}

func (dc StationConfig) Value() (driver.Value, error) {
	return json.Marshal(dc)
}

func (dc *StationConfig) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into DeviceConfig", value)
	}

	return json.Unmarshal(bytes, dc)
}

type Station struct {
	gorm.Model
	MacAddress string        `gorm:"uniqueIndex;not null" json:"mac_address"`
	TopicId    string        `grom:"not null" json:"topic_id"`
	Name       string        `gorm:"not null" json:"name"`
	Config     StationConfig `gorm:"type:jsonb" json:"config"`
	ClusterId  *uint         `json:"cluster_id,omitempty"`
	Cluster    *Cluster      `gorm:"foreignKey:ClusterId" json:"cluster,omitempty"`
}
