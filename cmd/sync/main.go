package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"gps-no-sync/internal/config"
	"gps-no-sync/internal/database/influx"
	"gps-no-sync/internal/database/postgres"
	"gps-no-sync/internal/database/postgres/events"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/logger"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/mqtt/handlers"
	"gps-no-sync/internal/services"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Application struct {
	config *config.Config

	postgresDB *postgres.PostgresDB
	influxDB   *influx.InfluxDB

	deviceRepository  *repositories.DeviceRepository
	clusterRepository *repositories.ClusterRepository

	deviceService  *services.DeviceService
	rangingService *services.RangingService

	mqttClient     *mqtt.Client
	topicManager   *mqtt.TopicManager
	deviceHandler  *handlers.DeviceHandler
	rangingHandler *handlers.RangingHandler

	shutdownChan chan os.Signal
	ctx          context.Context
	cancelFunc   context.CancelFunc
}

func main() {
	app := &Application{}

	if err := app.initialize(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application")
	}

	if err := app.run(); err != nil {
		log.Fatal().Err(err).Msg("Failed to run application")
	}
}

func (app *Application) initialize() error {
	var err error

	app.config, err = config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	logger.NewLogger(&app.config.Logger)
	log.Info().
		Str("service", "gps-no-sync").
		Str("version", "1.0.0").
		Msg("Setting up service...")

	app.ctx, app.cancelFunc = context.WithCancel(context.Background())
	app.shutdownChan = make(chan os.Signal, 1)
	signal.Notify(app.shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	if err := app.initializeDatabases(); err != nil {
		return fmt.Errorf("error while initialize databases: %w", err)
	}

	if err := app.initializeMQTT(); err != nil {
		return fmt.Errorf("error while initializing MQTT: %w", err)
	}

	if err := app.initializeServices(); err != nil {
		return fmt.Errorf("error while initializing services: %w", err)
	}

	log.Info().Msg("Successfully initialized application")
	return nil
}

func (app *Application) initializeDatabases() error {
	var err error

	app.postgresDB, err = postgres.NewConnection(&app.config.Postgres)
	if err != nil {
		return fmt.Errorf("could not connection to PostgreSQL: %w", err)
	}

	app.influxDB, err = influx.NewConnection(&app.config.InfluxDB)
	if err != nil {
		return fmt.Errorf("could not connect to InfluxDB: %w", err)
	}

	return nil
}

func (app *Application) initializeServices() error {
	deviceRepo := repositories.NewDeviceRepository(app.postgresDB.GetDB())

	measurementWriter := influx.NewRangingWriter(
		app.influxDB.GetWriteAPI(),
		logger.GetLogger("ranging-writer"),
	)

	app.deviceService = services.NewDeviceService(
		deviceRepo,
		logger.GetLogger("device-service"),
	)

	app.rangingService = services.NewRangingService(
		deviceRepo,
		measurementWriter,
		logger.GetLogger("ranging-service"),
	)

	deviceEventPublisher := services.NewDeviceEventPublisher(
		app.mqttClient,
		app.topicManager,
		logger.GetLogger("device-event-publisher"),
	)

	events.DeviceEventPublisher = deviceEventPublisher.PublishDeviceEvent

	log.Info().Msg("Successfully initialized services")
	return nil
}

func (app *Application) initializeMQTT() error {
	var err error

	app.mqttClient, err = mqtt.NewClient(&app.config.MQTT, logger.GetLogger("mqtt-client"))
	if err != nil {
		return fmt.Errorf("could not create MQTT client: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(app.ctx, 30*time.Second)
	defer cancel()

	if err := app.mqttClient.Connect(connectCtx); err != nil {
		return fmt.Errorf("could not connect to MQTT broker: %w", err)
	}

	app.topicManager = &mqtt.TopicManager{BaseTopic: app.config.MQTT.BaseTopic}

	app.deviceHandler = handlers.NewDeviceHandler(
		app.topicManager,
		app.deviceService,
		logger.GetLogger("device-handler"),
	)

	app.rangingHandler = handlers.NewRangingHandler(
		app.topicManager,
		app.rangingService,
		logger.GetLogger("ranging-handler"),
	)

	deviceTopic := app.topicManager.BuildDeviceTopicSubscription()
	if err := app.mqttClient.Subscribe(deviceTopic, app.deviceHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to Device Topic: %w", err)
	}

	rangingTopic := app.topicManager.BuildRangingTopicSubscription()
	if err := app.mqttClient.Subscribe(rangingTopic, app.rangingHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to Ranging Topic: %w", err)
	}

	log.Info().Msg("Successfully initialized MQTT client and handlers")
	return nil
}

func (app *Application) run() error {
	select {
	case sig := <-app.shutdownChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-app.ctx.Done():
		log.Info().Msg("context cancelled, shutting down application")
	}

	return app.shutdown()
}

func (app *Application) shutdown() error {
	if app.mqttClient != nil {
		app.mqttClient.Disconnect()
	}

	if app.influxDB != nil {
		app.influxDB.Close()
	}

	if app.postgresDB != nil {
		if err := app.postgresDB.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing PostgreSQL connection")
		}
	}

	app.cancelFunc()

	return nil
}
