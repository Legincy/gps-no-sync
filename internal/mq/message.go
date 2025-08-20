package mq

import (
	"gps-no-sync/internal/models"
)

type StationMessage struct {
	Data   models.StationDto `json:"data"`
	Source string            `json:"source"`
}

type StationConfig struct {
	UWB *UWB `json:"uwb"`
}

type UWB struct {
	Mode string `json:"mode"`
}

type ClusterMessage struct {
	Data   models.ClusterDto `json:"data"`
	Source string            `json:"source"`
	Topic  string            `json:"topic"`
}
