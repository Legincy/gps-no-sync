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
	"gps-no-sync/internal/mq"
	"gps-no-sync/internal/mq/handlers"
	"gps-no-sync/internal/services"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ApplicationImpl struct {
	configWrapper config.WrapperImpl

	postgresDB      *postgres.PostgresDB
	influxDB        *influxdb.InfluxDB
	listenerManager interfaces.IListenerManager

	stationRepository *repositories.StationRepository
	clusterRepository *repositories.ClusterRepository

	stationService     *services.StationService
	clusterService     *services.ClusterService
	measurementService *services.MeasurementService

	mqttClient         *mq.Client
	topicManager       *mq.TopicManager
	stationHandler     *handlers.StationHandler
	clusterHandler     *handlers.ClusterHandler
	measurementHandler *handlers.MeasurementHandler

	shutdownChan chan os.Signal
	ctx          context.Context
	cancelFunc   context.CancelFunc
}

func main() {
	app := &ApplicationImpl{}

	if err := app.initialize(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := app.stationService.SyncAll(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to sync all stations to MQTTConfig")
	}

	err = app.clusterService.SyncAll(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to sync all clusters to MQTTConfig")
	}

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

func (app *ApplicationImpl) setupTopicHandlers() error {
	app.stationHandler = handlers.NewStationHandler(
		app.stationService,
		logger.GetLogger("station-handler"),
		app.topicManager,
	)

	app.clusterHandler = handlers.NewClusterHandler(
		app.clusterService,
		logger.GetLogger("cluster-handler"),
		app.topicManager,
	)

	app.measurementHandler = handlers.NewMeasurementHandler(
		app.measurementService,
		logger.GetLogger("measurement-handler"),
		app.topicManager,
	)

	qos := app.configWrapper.MQTTConfig.QoS
	stationTopic := app.topicManager.GetStationTopic()
	if err := app.mqttClient.Subscribe(stationTopic, qos, app.stationHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to station Topic: %w", err)
	}

	clusterTopic := app.topicManager.GetClusterTopic()
	if err := app.mqttClient.Subscribe(clusterTopic, qos, app.clusterHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to cluster Topic: %w", err)
	}

	measurementTopic := app.topicManager.GetMeasurementTopic()
	if err := app.mqttClient.Subscribe(measurementTopic, qos, app.measurementHandler.HandleMessage); err != nil {
		return fmt.Errorf("error subscribing to measurement Topic: %w", err)
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

	stationListener := listeners.NewStationTableListener(
		logger.GetLogger("station-listener"),
		app.mqttClient,
		app.topicManager,
		app.stationService,
		app.stationRepository,
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

func (app *ApplicationImpl) initializeRepositories() error {
	db := app.postgresDB.GetDB()

	app.stationRepository = repositories.NewStationRepository(db)
	app.clusterRepository = repositories.NewClusterRepository(db)

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized repositories")
	return nil
}

func (app *ApplicationImpl) initializeServices() error {
	app.clusterService = services.NewClusterService(
		app.clusterRepository,
		app.mqttClient,
		app.topicManager,
		logger.GetLogger("cluster-service"),
	)

	app.stationService = services.NewStationService(
		app.stationRepository,
		app.clusterService,
		app.clusterRepository,
		app.mqttClient,
		app.topicManager,
		logger.GetLogger("station-service"),
	)

	app.measurementService = services.NewMeasurementService(
		app.influxDB,
		app.topicManager,
		logger.GetLogger("measurement-service"),
	)

	log.Info().
		Str("component", "main").
		Msg("Successfully initialized services")
	return nil
}

func (app *ApplicationImpl) initializeMQTT() error {
	var err error

	baseTopic := app.configWrapper.MQTTConfig.BaseTopic
	app.topicManager = mq.NewTopicManager(baseTopic, logger.GetLogger("topic-manager"))

	app.mqttClient, err = mq.NewClient(&app.configWrapper.MQTTConfig, logger.GetLogger("mqtt-client"))
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

	if app.mqttClient != nil {
		app.mqttClient.Disconnect(app.ctx)
	}

	if app.postgresDB != nil {
		if err := app.postgresDB.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing PostgresQL connection")
		}
	}

	app.cancelFunc()
	return nil
}
