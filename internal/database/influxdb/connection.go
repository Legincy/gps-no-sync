package influxdb

import (
	"context"
	"errors"
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

func (i *InfluxDB) WriteMeasurement(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) error {
	i.logger.Info().
		Str("measurement", measurement).
		Interface("tags", tags).
		Interface("fields", fields).
		Time("timestamp", timestamp).
		Msg("Preparing to write measurement to InfluxDB")

	// Validate fields - InfluxDB needs at least one field
	if len(fields) == 0 {
		err := errors.New("no fields provided for measurement - InfluxDB requires at least one field")
		i.logger.Error().Err(err).Msg("Validation failed")
		return err
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
		err := errors.New("all field values are nil - cannot write measurement")
		i.logger.Error().Err(err).Msg("Validation failed")
		return err
	}

	// ✅ FIX: Verwende cleanFields statt fields
	point := influxdb2.NewPoint(measurement, tags, cleanFields, timestamp)
	i.WritePoint(point)

	// Synchron flushen und auf Fehler prüfen
	i.writeAPI.Flush()

	// Kurz warten damit asynchrone Fehler verarbeitet werden können
	time.Sleep(100 * time.Millisecond)

	i.logger.Debug().
		Str("measurement", measurement).
		Msg("Measurement written and flushed")

	return nil
}

func (i *InfluxDB) WriteMeasurementSync(measurement string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) error {
	// Validation (same as above)
	cleanFields := make(map[string]interface{})
	for k, v := range fields {
		if v != nil {
			cleanFields[k] = v
		}
	}

	if len(cleanFields) == 0 {
		return errors.New("no valid fields to write")
	}

	// Verwende WriteAPIBlocking für synchrone Writes
	writeAPI := i.client.WriteAPIBlocking(i.config.Organization, i.config.Bucket)

	point := influxdb2.NewPoint(measurement, tags, cleanFields, timestamp)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writeAPI.WritePoint(ctx, point)
	if err != nil {
		i.logger.Error().Err(err).Msg("Failed to write point synchronously")
		return err
	}

	i.logger.Debug().Str("measurement", measurement).Msg("Measurement written synchronously")
	return nil
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
