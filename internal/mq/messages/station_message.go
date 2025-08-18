package messages

import (
	"fmt"
	"gps-no-sync/internal/models"
	"strings"
)

type StationMessage struct {
	Data   StationDto `json:"data"`
	Source string     `json:"source"`
}

func (sm *StationMessage) Validate() error {
	if sm.Data.MacAddress == "" {
		return fmt.Errorf("mac_address is required")
	}

	return nil
}

type StationDto struct {
	Topic      string        `json:"topic"`
	MacAddress string        `json:"mac_address"`
	Name       string        `json:"name,omitempty"`
	ClusterID  uint          `json:"cluster_id,omitempty"`
	Uptime     *uint64       `json:"uptime,omitempty"`
	Config     StationConfig `json:"config"`
}

type StationConfig struct {
	UWB *UWB `json:"uwb"`
}

type UWB struct {
	Mode string `json:"mode"`
}

func (s *StationDto) ToModel() (models.Station, error) {
	var stationConfig models.StationConfig

	if s.Config.UWB != nil {
		stationConfig.UWB = &models.UWBConfig{}
		switch strings.ToUpper(strings.TrimSpace(s.Config.UWB.Mode)) {
		case "ANCHOR":
			stationConfig.UWB.Mode = models.DW3000ModeAnchor
		case "TAG":
			stationConfig.UWB.Mode = models.DW3000ModeTag
		case "UNKNOWN", "":
			stationConfig.UWB.Mode = models.DW3000ModeUnknown
		default:
			stationConfig.UWB.Mode = models.DW3000ModeUnknown
		}
	}

	station := models.Station{
		MacAddress: s.MacAddress,
		Topic:      s.Topic,
		ClusterID:  &s.ClusterID,
		Name:       s.Name,
		Config:     stationConfig,
	}

	return station, nil
}
