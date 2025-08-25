package models

import (
	"bytes"
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
	CreatedAt  *time.Time    `json:"created_at"`
	UpdatedAt  *time.Time    `json:"updated_at"`
	DeletedAt  *time.Time    `gorm:"index" json:"deleted_at"`
	MacAddress string        `gorm:"uniqueIndex;not null" json:"mac_address"`
	Topic      string        `json:"topic"`
	Name       string        `json:"name"`
	Config     StationConfig `gorm:"type:jsonb" json:"config"`
	ClusterID  *uint         `json:"cluster_id"`
	Cluster    *Cluster      `gorm:"foreignKey:ClusterID"`
}

func (s *Station) IsValid() bool {
	return s.Name != "" && s.Topic != ""
}

func (s *Station) LoadDefault() {
	s.MacAddress = strings.ToLower(s.MacAddress)
	if len(s.MacAddress) == 12 && !strings.Contains(s.MacAddress, ":") {
		s.MacAddress = fmt.Sprintf("%s:%s:%s:%s:%s:%s",
			s.MacAddress[0:2], s.MacAddress[2:4], s.MacAddress[4:6],
			s.MacAddress[6:8], s.MacAddress[8:10], s.MacAddress[10:12])
	}

	if s.Name == "" {
		identifier := strings.ToUpper(fmt.Sprintf("%s%s%s", s.MacAddress[9:11], s.MacAddress[12:14], s.MacAddress[15:17]))
		s.Name = fmt.Sprintf("GPS:No Station-%s", identifier)
	}

	if s.Topic == "" {
		s.Topic = fmt.Sprintf("%s%s%s%s%s%s",
			s.MacAddress[0:2], s.MacAddress[3:5], s.MacAddress[6:8],
			s.MacAddress[9:11], s.MacAddress[12:14], s.MacAddress[15:17])
	}

	if s.CreatedAt == nil {
		now := time.Now()
		s.CreatedAt = &now
	}

	fmt.Println(s)
}

func (s *Station) UpdateFromDto(dto *StationDto) {
	if dto == nil {
		return
	}

	s.MacAddress = dto.MacAddress
	s.Name = dto.Name
	s.ClusterID = dto.ClusterID
	s.Config = dto.Config
}

func (s *Station) IsEqual(other StationDto) bool {
	stationDto := s.ToDto()

	byteStation, err1 := json.Marshal(stationDto)
	byteOther, err2 := json.Marshal(other)

	if err1 != nil || err2 != nil {
		return false
	}

	return bytes.Equal(byteStation, byteOther)
}

func (s *Station) ToDto() *StationDto {
	return &StationDto{
		MacAddress: s.MacAddress,
		Name:       s.Name,
		ClusterID:  s.ClusterID,
		Config:     s.Config,
	}
}

type StationDto struct {
	MacAddress string        `json:"mac_address"`
	Name       string        `json:"name"`
	ClusterID  *uint         `json:"cluster_id"`
	Config     StationConfig `json:"config"`
}

func (s *StationDto) ToStation() *Station {
	return &Station{
		MacAddress: s.MacAddress,
		Name:       s.Name,
		ClusterID:  s.ClusterID,
		Config:     s.Config,
	}
}

func (s *StationDto) GetMergedMacAddress() string {
	return fmt.Sprintf("%s%s%s%s%s%s",
		s.MacAddress[0:2], s.MacAddress[3:5], s.MacAddress[6:8],
		s.MacAddress[9:11], s.MacAddress[12:14], s.MacAddress[15:17])
}
