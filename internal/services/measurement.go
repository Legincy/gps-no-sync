package services

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/database/influxdb"
	"gps-no-sync/internal/models"
	"strings"
	"sync"
	"time"
)

type MeasurementService struct {
	influxDB *influxdb.InfluxDB
	logger   zerolog.Logger

	batchSize     int
	flushInterval time.Duration
	buffer        []*models.Measurement
	bufferMu      sync.Mutex
}

func NewMeasurementService(
	influxDB *influxdb.InfluxDB,
	logger zerolog.Logger,
) *MeasurementService {
	return &MeasurementService{
		influxDB:      influxDB,
		logger:        logger.With().Str("service", "measurement").Logger(),
		batchSize:     100,
		flushInterval: 5 * time.Second,
		buffer:        make([]*models.Measurement, 0, 100),
	}
}

func (s *MeasurementService) StoreBatch(ctx context.Context, measurements []*models.Measurement) error {
	if len(measurements) == 0 {
		return nil
	}

	for i, m := range measurements {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("measurement[%d] invalid: %w", i, err)
		}
	}

	errCh := make(chan error, len(measurements))
	var wg sync.WaitGroup

	chunkSize := 50
	for i := 0; i < len(measurements); i += chunkSize {
		end := i + chunkSize
		if end > len(measurements) {
			end = len(measurements)
		}

		chunk := measurements[i:end]

		wg.Add(1)
		go func(chunk []*models.Measurement) {
			defer wg.Done()

			for _, m := range chunk {
				if err := s.storeSingle(ctx, m); err != nil {
					errCh <- err
					return
				}
			}
		}(chunk)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return fmt.Errorf("batch storage failed: %w", err)
		}
	}

	s.logger.Info().
		Int("count", len(measurements)).
		Msg("Batch stored successfully")

	return nil
}

func (s *MeasurementService) Store(ctx context.Context, measurement *models.Measurement) error {
	if err := measurement.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	return s.storeSingle(ctx, measurement)
}

func (s *MeasurementService) storeSingle(ctx context.Context, m *models.Measurement) error {
	fmt.Println(m)
	measurementName := fmt.Sprintf("measurement_%s", strings.ToLower(string(m.Type)))

	tags := m.ToInfluxTags()
	fields := m.ToInfluxFields()

	if err := s.influxDB.WriteMeasurement(measurementName, tags, fields, m.Timestamp); err != nil {
		s.logger.Error().Err(err).
			Str("station_id", m.StationID).
			Str("type", string(m.Type)).
			Msg("Failed to write to InfluxDB")
		return fmt.Errorf("influx write failed: %w", err)
	}

	return nil
}

func (s *MeasurementService) GetLatest(ctx context.Context, stationID string, limit int) ([]*models.Measurement, error) {
	query := fmt.Sprintf(`
        from(bucket: "measurements")
        |> range(start: -24h)
        |> filter(fn: (r) => r.station_id == "%s")
        |> sort(columns: ["_time"], desc: true)
        |> limit(n: %d)
    `, stationID, limit)

	result, err := s.influxDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var measurements []*models.Measurement

	for result.Next() {
		record := result.Record()

		measurement := &models.Measurement{
			StationID: record.ValueByKey("station_id").(string),
			Type:      models.MeasurementType(record.ValueByKey("type").(string)),
			Value:     record.Value().(float64),
			Unit:      record.ValueByKey("unit").(string),
			Timestamp: record.Time(),
		}

		if target, ok := record.ValueByKey("target").(string); ok {
			measurement.Target = target
		}

		measurements = append(measurements, measurement)
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("result error: %w", result.Err())
	}

	return measurements, nil
}
