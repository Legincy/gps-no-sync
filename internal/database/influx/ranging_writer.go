package influx

import (
	"context"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/models"
)

type RangingWriter struct {
	writeAPI api.WriteAPI
	logger   zerolog.Logger
}

func NewRangingWriter(writeAPI api.WriteAPI, logger zerolog.Logger) *RangingWriter {
	return &RangingWriter{
		writeAPI: writeAPI,
		logger:   logger,
	}
}

func (w *RangingWriter) WriteRangingMeasurement(ctx context.Context, measurement *models.RangingData) error {
	tags := map[string]string{
		"source_device":      measurement.SourceDevice,
		"destination_device": measurement.DestinationDevice,
	}

	fields := map[string]interface{}{
		"raw_distance": measurement.Distance.RawDistance,
	}

	point := influxdb2.NewPoint(
		"ranging_data",
		tags,
		fields,
		measurement.Timestamp,
	)

	w.writeAPI.WritePoint(point)

	w.logger.Debug().
		Str("source", measurement.SourceDevice).
		Str("destination", measurement.DestinationDevice).
		Float64("raw_distance", measurement.Distance.RawDistance).
		Msg("Added ranging data to influxDB")

	return nil
}

func (w *RangingWriter) WriteBatchRangingMeasurements(ctx context.Context, measurements []*models.RangingData) error {
	for _, measurement := range measurements {
		if err := w.WriteRangingMeasurement(ctx, measurement); err != nil {
			return err
		}
	}
	return nil
}
