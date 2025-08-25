package influxdb

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/config"
	"time"
)

type InfluxDB struct {
	client     influxdb2.Client
	writeAPI   api.WriteAPI
	queryAPI   api.QueryAPI
	config     *config.InfluxConfig
	logger     zerolog.Logger
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewConnection(cfg *config.InfluxConfig, logger zerolog.Logger) (*InfluxDB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := influxdb2.NewClient(cfg.URL, cfg.Token)

	writeAPI := client.WriteAPI(cfg.Organization, cfg.Bucket)
	queryAPI := client.QueryAPI(cfg.Organization)

	health, err := client.Health(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}

	if health.Status != "pass" {
		return nil, fmt.Errorf("InfluxDB health check failed: %s", health.Status)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	influxDB := &InfluxDB{
		client:     client,
		writeAPI:   writeAPI,
		queryAPI:   queryAPI,
		config:     cfg,
		logger:     logger,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}

	go influxDB.handleWriteErrors()

	logger.Info().
		Str("component", "influxdb").
		Str("url", cfg.URL).
		Str("organization", cfg.Organization).
		Str("bucket", cfg.Bucket).
		Msg("Successfully connected to InfluxDB")

	return influxDB, nil
}

func (i *InfluxDB) handleWriteErrors() {
	errorsCh := i.writeAPI.Errors()
	for {
		select {
		case err := <-errorsCh:
			i.logger.Error().Err(err).
				Str("component", "influxdb").
				Msg("Write error occurred")
		case <-i.ctx.Done():
			return
		}
	}
}

func (i *InfluxDB) WritePoint(point *write.Point) {
	i.writeAPI.WritePoint(point)
}

func (i *InfluxDB) WriteMeasurement(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) {
	i.logger.Info().
		Str("measurement", measurement).
		Interface("tags", tags).
		Interface("fields", fields).
		Time("timestamp", timestamp).
		Msg("Preparing to write measurement to InfluxDB")

	// Validate fields - InfluxDB needs at least one field
	if len(fields) == 0 {
		i.logger.Error().Msg("No fields provided for measurement - InfluxDB requires at least one field")
		return
	}

	// Check for nil values in fields
	cleanFields := make(map[string]interface{})
	for k, v := range fields {
		if v != nil {
			cleanFields[k] = v
		} else {
			i.logger.Warn().Str("field", k).Msg("Skipping nil field value")
		}
	}

	if len(cleanFields) == 0 {
		i.logger.Error().Msg("All field values are nil - cannot write measurement")
		return
	}

	point := influxdb2.NewPoint(measurement, tags, fields, timestamp)
	i.WritePoint(point)

	i.writeAPI.Flush()

	i.logger.Debug().
		Str("measurement", measurement).
		Msg("Measurement written and flushed")
}

func (i *InfluxDB) Flush() {
	i.writeAPI.Flush()
}

func (i *InfluxDB) Query(query string) (*api.QueryTableResult, error) {
	return i.queryAPI.Query(i.ctx, query)
}

func (i *InfluxDB) Close() {
	i.writeAPI.Flush()
	i.cancelFunc()
	i.client.Close()

	i.logger.Info().
		Str("component", "influxdb").
		Msg("InfluxDB connection closed")
}
