package models

import (
	"fmt"
	"time"
)

type MeasurementType string

const (
	MeasurementTypeUWB MeasurementType = "UWB"
)

type Measurement struct {
	StationID string                 `json:"station_id"`
	Type      MeasurementType        `json:"type"`
	Value     float64                `json:"value"`
	Unit      string                 `json:"unit"`
	Target    string                 `json:"target,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

func (m *Measurement) ToInfluxTags() map[string]string {
	tags := map[string]string{
		"station_id": m.StationID,
		"type":       string(m.Type),
		"unit":       m.Unit,
	}

	if m.Target != "" {
		tags["target"] = m.Target
	}

	return tags
}

func (m *Measurement) ToInfluxFields() map[string]interface{} {
	fields := map[string]interface{}{
		"value": m.Value,
	}

	for k, v := range m.Metadata {
		fields[fmt.Sprintf("meta_%s", k)] = v
	}

	return fields
}

func (m *Measurement) Validate() error {
	if m.StationID == "" {
		return fmt.Errorf("station_id is required")
	}
	if m.Type == "" {
		return fmt.Errorf("type is required")
	}
	if m.Unit == "" {
		return fmt.Errorf("unit is required")
	}
	if m.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}
