package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
	ID         uint          `gorm:"primaryKey" json:"id"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	DeletedAt  time.Time     `gorm:"index" json:"deleted_at"`
	MacAddress string        `gorm:"uniqueIndex;not null" json:"mac_address"`
	Topic      string        `json:"topic"`
	Name       string        `json:"name"`
	Config     StationConfig `gorm:"type:jsonb" json:"config"`
	ClusterID  *uint         `json:"cluster_id"`
	Cluster    *Cluster      `gorm:"foreignKey:ClusterID"`
}

type StationMqDto struct {
	MacAddress string        `json:"mac_address"`
	Name       string        `json:"name"`
	ClusterID  *uint         `json:"cluster_id"`
	Config     StationConfig `json:"config"`
}

func (s *Station) ToMqDto() *StationMqDto {
	return &StationMqDto{
		MacAddress: s.MacAddress,
		Name:       s.Name,
		Config:     s.Config,
		ClusterID:  s.ClusterID,
	}
}

func (s *Station) GetMergedMacAddress() string {
	return fmt.Sprintf("%s%s%s%s%s%s",
		s.MacAddress[0:2], s.MacAddress[3:5], s.MacAddress[6:8],
		s.MacAddress[9:11], s.MacAddress[12:14], s.MacAddress[15:17])
}

func (s *Station) Equals(other *Station) (bool, error) {
	if s == nil || other == nil {
		return false, nil
	}

	byteStation, err := json.Marshal(s)
	if err != nil {
		return false, fmt.Errorf("error marshalling station: %w", err)
	}

	byteOther, err := json.Marshal(other)
	if err != nil {
		return false, fmt.Errorf("error marshalling other station: %w", err)
	}

	return string(byteStation) == string(byteOther), nil
}

func (s *Station) Standardize() (*Station, error) {
	validatedStation := *s

	if validatedStation.MacAddress == "" {
		return nil, fmt.Errorf("mac_address is required")
	}

	if validatedStation.Topic == "" {
		validatedStation.Topic = s.GetMergedMacAddress()
	}

	if validatedStation.Name == "" {
		name := strings.ToUpper(s.GetMergedMacAddress())
		validatedStation.Name = fmt.Sprintf("GPS:No Station-%s", name[len(name)-6:])
	}

	return &validatedStation, nil
}
