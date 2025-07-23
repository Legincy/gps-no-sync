package models

import "time"

type RangingData struct {
	SourceDevice      string `json:"source_device"`
	DestinationDevice string `json:"destination_device"`
	Distance          struct {
		RawDistance float64 `json:"raw_distance"`
	} `json:"distance"`
	Timestamp time.Time `json:"timestamp"`
}

type RangingMeasurement struct {
	SourceDevice      string    `json:"source_device"`
	DestinationDevice string    `json:"destination_device"`
	RawDistance       float64   `json:"raw_distance"`
	Timestamp         time.Time `json:"timestamp"`
}

type RangingDataArray []RangingData
