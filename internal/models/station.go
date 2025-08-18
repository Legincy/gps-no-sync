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
	Topic      string        `grom:"not null" json:"topic"`
	Name       string        `gorm:"not null" json:"name"`
	Config     StationConfig `gorm:"type:jsonb" json:"config"`
	ClusterId  *uint         `json:"cluster_id,omitempty"`
	Cluster    *Cluster      `gorm:"foreignKey:ClusterId" json:"cluster,omitempty"`
}

type StationMqDto struct {
	MacAddress string        `json:"mac_address"`
	Topic      string        `json:"topic"`
	Name       string        `json:"name"`
	ClusterId  *uint         `json:"cluster_id"`
	Config     StationConfig `json:"config"`
}

func (s *Station) ToMqDto() *StationMqDto {
	return &StationMqDto{
		MacAddress: s.MacAddress,
		Topic:      s.Topic,
		Name:       s.Name,
		Config:     s.Config,
		ClusterId:  s.ClusterId,
	}
}
