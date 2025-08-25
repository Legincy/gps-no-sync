package models

import (
	"fmt"
	"time"
)

type MeasurementType string

const (
	MeasurementTypeUWBDistance MeasurementType = "uwb"
)

type Measurement struct {
	ID         string                 `json:"id"`
	StationID  string                 `json:"station_id"`
	Type       MeasurementType        `json:"type"`
	Value      interface{}            `json:"value"`
	Unit       string                 `json:"unit"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
	ReceivedAt time.Time              `json:"received_at"`
}

type UWBDistanceMeasurement struct {
	Distance  float64 `json:"distance"`
	Quality   float64 `json:"quality"`
	TargetID  string  `json:"target_id"`
	RSSI      int     `json:"rssi"`
	FirstPath float64 `json:"first_path"`
	RxPower   float64 `json:"rx_power"`
}

func (m *Measurement) GetFields() map[string]interface{} {
	fields := make(map[string]interface{})

	switch m.Type {
	case MeasurementTypeUWBDistance:
		if uwbData, ok := m.Value.(UWBDistanceMeasurement); ok {
			fields["distance"] = uwbData.Distance
			fields["quality"] = uwbData.Quality
			fields["rssi"] = uwbData.RSSI
			fields["first_path"] = uwbData.FirstPath
			fields["rx_power"] = uwbData.RxPower
		} else if uwbMap, ok := m.Value.(map[string]interface{}); ok {
			for k, v := range uwbMap {
				if k != "target_id" { // target_id is a tag, not a field
					fields[k] = v
				}
			}
		}
	default:
		// For unknown types, try to serialize as JSON or use direct value
		if valueMap, ok := m.Value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				fields[k] = v
			}
		} else {
			fields["value"] = m.Value
		}
	}

	// Add metadata as fields with metadata_ prefix
	for k, v := range m.Metadata {
		fields["metadata_"+k] = v
	}

	fields["unit"] = m.Unit

	return fields
}

func (m *Measurement) GetTags() map[string]string {
	tags := map[string]string{
		"station_id":       m.StationID,
		"measurement_type": string(m.Type),
	}

	switch m.Type {
	case MeasurementTypeUWBDistance:
		if uwbData, ok := m.Value.(UWBDistanceMeasurement); ok {
			tags["target_id"] = uwbData.TargetID
		} else if uwbMap, ok := m.Value.(map[string]interface{}); ok {
			if targetID, exists := uwbMap["target_id"]; exists {
				tags["target_id"] = fmt.Sprintf("%v", targetID)
			}
		}
	}

	return tags
}

func (m *Measurement) Validate() error {
	if m.StationID == "" {
		return fmt.Errorf("station_id is required")
	}
	if m.Type == "" {
		return fmt.Errorf("measurement type is required")
	}
	if m.Value == nil {
		return fmt.Errorf("measurement value is required")
	}
	if m.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}
