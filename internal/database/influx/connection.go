package influx

import (
	"context"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"gps-no-sync/internal/config"
	"time"
)

type InfluxDB struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
	config   *config.InfluxConfig
}

func NewConnection(cfg *config.InfluxConfig) (*InfluxDB, error) {
	client := influxdb2.NewClient(cfg.URL, cfg.Token)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	health, err := client.Health(ctx)
	if err != nil {
		return nil, fmt.Errorf("error connecting to InfluxDB: %v", err)
	}

	if health.Status != "pass" {
		return nil, fmt.Errorf("InfluxDB health check failed: %s", health.Status)
	}

	writeAPI := client.WriteAPI(cfg.Organization, cfg.Bucket)

	influxDB := &InfluxDB{
		client:   client,
		writeAPI: writeAPI,
		config:   cfg,
	}

	return influxDB, nil
}

func (i *InfluxDB) GetWriteAPI() api.WriteAPI {
	return i.writeAPI
}

func (i *InfluxDB) Close() {
	i.writeAPI.Flush()
	i.client.Close()
}
