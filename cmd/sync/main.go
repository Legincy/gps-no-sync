package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"gps-no-sync/internal/config"
	"gps-no-sync/internal/database/influx"
	"gps-no-sync/internal/database/postgres"
	"gps-no-sync/internal/database/postgres/listeners"
	"gps-no-sync/internal/database/postgres/repositories"
	"gps-no-sync/internal/logger"
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/mq/handlers"
	"gps-no-sync/internal/services"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Application struct {
	config *config.Config

	postgresDB      *postgres.PostgresDB
	listenerManager *listeners.ListenerManager
	influxDB        *influx.InfluxDB

	stationRepository *repositories.StationRepository
	clusterRepository *repositories.ClusterRepository

	stationService *services.StationService
	clusterService *services.ClusterService

	mqttClient     *mq.Client
	topicManager   *mq.TopicManager
	stationHandler *handlers.StationHandler

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

	logger.NewLogger(app.config.Logger)
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

	if err := app.initializeMQTT(); err != nil {
		return fmt.Errorf("error while initializing MQTT: %w", err)
	}

	if err := app.initializeRepositories(); err != nil {
		return fmt.Errorf("error while initializing repositories: %w", err)
	}

	if err := app.initializeServices(); err != nil {
		return fmt.Errorf("error while initializing services: %w", err)
	}

	if err := app.setupTopicHandlers(); err != nil {
		return fmt.Errorf("error while setting up topic handlers: %w", err)
	}

	if err := app.setupTableListeners(); err != nil {
		return fmt.Errorf("error while setting up table listeners: %w", err)
	}
	log.Info().Msg("Successfully initialized application")
	return nil
}

func (app *Application) initializeDatabases() error {
	var err error

	app.postgresDB, err = postgres.NewConnection(app.config.Postgres)
	if err != nil {
		return fmt.Errorf("could not connection to PostgreSQL: %w", err)
	}

	app.influxDB, err = influx.NewConnection(&app.config.InfluxDB)
	if err != nil {
		return fmt.Errorf("could not connect to InfluxDB: %w", err)
	}

	log.Info().
		Str("component", "main").
		Str("host", app.config.Postgres.Host).
		Msg("Successfully initialized databases")
	return nil
}

func (app *Application) setupTopicHandlers() error {
	app.stationHandler = handlers.NewStationHandler(
		app.topicManager,
		app.stationService,
		logger.GetLogger("station-handler"),
	)

	stationTopic := app.topicManager.GetStationTopic()
	if err := app.mqttClient.Subscribe(stationTopic, app.stationHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to station Topic: %w", err)
	}

	return nil
}

func (app *Application) setupTableListeners() error {
	app.listenerManager = listeners.NewListenerManager(
		app.postgresDB.GetDB(),
		&app.config.Postgres,
		logger.GetLogger("listener-manager"),
	)

	stationListener := listeners.NewStationTableListener(
		logger.GetLogger("station-listener"),
		app.mqttClient,
		app.topicManager,
		app.stationService,
	)
	if err := app.listenerManager.RegisterListener(stationListener); err != nil {
		return fmt.Errorf("failed to register station listener: %w", err)
	}

	clusterListener := listeners.NewClusterTableListener(
		logger.GetLogger("cluster-listener"),
		app.mqttClient,
		app.topicManager,
		app.clusterService,
	)
	if err := app.listenerManager.RegisterListener(clusterListener); err != nil {
		return fmt.Errorf("failed to register cluster listener: %w", err)
	}

	if err := app.listenerManager.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize listener manager: %w", err)
	}

	app.listenerManager.Start()

	log.Info().Msg("All table listeners initialized and started")
	return nil
}

func (app *Application) initializeRepositories() error {
	db := app.postgresDB.GetDB()

	app.stationRepository = repositories.NewStationRepository(db)
	app.clusterRepository = repositories.NewClusterRepository(db)

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized repositories")
	return nil
}

func (app *Application) initializeServices() error {

	app.stationService = services.NewStationService(
		app.stationRepository,
		app.clusterRepository,
		app.mqttClient,
		app.topicManager,
		logger.GetLogger("station-service"),
	)

	app.clusterService = services.NewClusterService(
		app.clusterRepository,
		app.mqttClient,
		app.topicManager,
		logger.GetLogger("cluster-service"),
	)

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized services")
	return nil
}

func (app *Application) initializeMQTT() error {
	var err error

	app.topicManager = &mq.TopicManager{BaseTopic: app.config.MQTT.BaseTopic}

	app.mqttClient, err = mq.NewClient(&app.config.MQTT, logger.GetLogger("mq-client"))
	if err != nil {
		return fmt.Errorf("could not create MQTT client: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(app.ctx, 30*time.Second)
	defer cancel()

	if err := app.mqttClient.Connect(connectCtx); err != nil {
		return fmt.Errorf("could not connect to MQTT broker: %w", err)
	}

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized MQTT client")

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
	if app.listenerManager != nil {
		app.listenerManager.Stop()
	}

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
