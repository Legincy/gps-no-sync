package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"gps-no-sync/internal/config"
	"gps-no-sync/internal/database/influxdb"
	"gps-no-sync/internal/database/postgres"
	"gps-no-sync/internal/database/postgres/listeners"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/interfaces"
	"gps-no-sync/internal/logger"
	"gps-no-sync/internal/mqtt"
	"gps-no-sync/internal/mqtt/handlers"
	"gps-no-sync/internal/services"
	"os"
	"os/signal"
	"syscall"
)

type ApplicationImpl struct {
	configWrapper config.WrapperImpl

	broker *mqtt.BrokerImpl

	postgresDB      *postgres.PostgresDB
	influxDB        *influxdb.InfluxDB
	listenerManager interfaces.IListenerManager

	stationRepository *repositories.StationRepository

	stationService     *services.StationService
	measurementService *services.MeasurementService

	shutdownChan chan os.Signal
	ctx          context.Context
	cancelFunc   context.CancelFunc
}

func main() {
	app := &ApplicationImpl{}

	if err := app.initialize(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application")
	}

	stationList, err := app.stationService.GetAll(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve stations from database")
	}
	publisher := app.broker.GetPublisher()
	err = publisher.PublishStationList(stationList)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish station list to MQTT")
	}

	/*
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
			err := app.stationService.SyncAll(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to sync all stations to MQTTConfig")
			}
	*/

	if err := app.run(); err != nil {
		log.Fatal().Err(err).Msg("Failed to run application")
	}
}

func (app *ApplicationImpl) initialize() error {
	app.configWrapper = config.NewWrapper()

	logger.NewLogger(&app.configWrapper.LoggerConfig)
	log.Info().
		Str("component", "main").
		Str("version", "1.0.0").
		Msg("Setting up service...")

	app.ctx, app.cancelFunc = context.WithCancel(context.Background())
	app.shutdownChan = make(chan os.Signal, 1)
	signal.Notify(app.shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	if err := app.initializeDatabases(); err != nil {
		return fmt.Errorf("error while initialize databases: %w", err)
	}

	if err := app.initializeMqtt(); err != nil {
		return fmt.Errorf("error while initializing MQTT: %w", err)
	}

	if err := app.initializeRepositories(); err != nil {
		return fmt.Errorf("error while initializing repositories: %w", err)
	}

	if err := app.initializeServices(); err != nil {
		return fmt.Errorf("error while initializing services: %w", err)
	}

	if err := app.registerHandlers(); err != nil {
		return fmt.Errorf("error while registering handlers: %w", err)
	}

	/*if err := app.setupTopicHandlers(); err != nil {
		return fmt.Errorf("error while setting up topic handlers: %w", err)
	}

	*/

	if err := app.setupTableListeners(); err != nil {
		return fmt.Errorf("error while setting up table listeners: %w", err)
	}
	log.Info().Msg("Successfully initialized application")
	return nil
}

func (app *ApplicationImpl) initializeDatabases() error {
	var err error

	app.postgresDB, err = postgres.NewConnection(&app.configWrapper.PostgresConfig)
	if err != nil {
		return fmt.Errorf("could not connection to PostgreSQL: %w", err)
	}

	app.influxDB, err = influxdb.NewConnection(&app.configWrapper.InfluxConfig, logger.GetLogger("influxdb"))
	if err != nil {
		return fmt.Errorf("could not connect to InfluxConfig: %w", err)
	}

	return nil
}

func (app *ApplicationImpl) setupTableListeners() error {
	dsn := app.configWrapper.PostgresConfig.Dsn

	app.listenerManager = listeners.NewListenerManager(
		app.postgresDB.GetDB(),
		dsn,
		logger.GetLogger("listener-manager"),
	)

	/*
		stationListener := listeners.NewStationTableListener(
			logger.GetLogger("station-listener"),

			app.stationService,
			app.stationRepository,
		)
		if err := app.listenerManager.RegisterListener(stationListener); err != nil {
			return fmt.Errorf("failed to register station listener: %w", err)
		}
	*/

	if err := app.listenerManager.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize listener manager: %w", err)
	}

	app.listenerManager.Start()

	log.Info().Msg("All table listeners initialized and started")
	return nil
}

func (app *ApplicationImpl) initializeRepositories() error {
	db := app.postgresDB.GetDB()

	app.stationRepository = repositories.NewStationRepository(db)

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized repositories")
	return nil
}

func (app *ApplicationImpl) initializeServices() error {

	app.stationService = services.NewStationService(
		app.stationRepository,
		logger.GetLogger("station-service"),
	)

	/*
		app.measurementService = services.NewMeasurementService(
			app.influxDB,
			app.topicManager,
			logger.GetLogger("measurement-service"),
		)

	*/

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized services")
	return nil
}

func (app *ApplicationImpl) initializeMqtt() error {
	app.broker = mqtt.NewBroker(&app.configWrapper.MQTTConfig, log.Logger)

	ctx := context.Background()
	if err := app.broker.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to start MQTT broker")
	}

	log.Info().Msg("GPS-No-Sync service started successfully")

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized MQTT client")

	return nil
}

func (app *ApplicationImpl) registerHandlers() error {
	sHandler := handlers.NewStationHandler(
		app.stationService,
		app.broker.GetPublisher(),
		log.Logger,
	)

	router := app.broker.GetRouter()
	router.RegisterHandler("gpsno/v2/stations", sHandler)

	subscriber := app.broker.GetSubscriber()
	if err := subscriber.Subscribe("gpsno/v2/stations", 1); err != nil {
		log.Fatal().Err(err).Msg("Failed to subscribe to stations topic")
	}

	log.Info().
		Str("component", "main").
		Msg("Successfully registered MQTT topic handlers")

	return nil
}

func (app *ApplicationImpl) run() error {
	select {
	case sig := <-app.shutdownChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-app.ctx.Done():
		log.Info().Msg("context cancelled, shutting down application")
	}

	return app.shutdown()
}

func (app *ApplicationImpl) shutdown() error {
	if app.listenerManager != nil {
		app.listenerManager.Stop()
	}

	if app.postgresDB != nil {
		if err := app.postgresDB.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing PostgresQL connection")
		}
	}

	app.cancelFunc()
	return nil
}
