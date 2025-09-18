package mqtt

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/config/components"
	"sync"
	"time"
)

type BrokerImpl struct {
	client mqtt.Client
	logger zerolog.Logger

	publisher  *PublisherImpl
	subscriber *SubscriberImpl
	router     *RouterImpl

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.RWMutex
	started bool
}

func NewBroker(cfg *components.MQTTConfigImpl, logger zerolog.Logger) *BrokerImpl {
	ctx, cancel := context.WithCancel(context.Background())

	broker := &BrokerImpl{
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", cfg.Host, cfg.Port))
	opts.SetClientID(cfg.ClientID)
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)
	opts.SetKeepAlive(cfg.KeepAlive)
	opts.SetAutoReconnect(cfg.AutoReconnect)
	opts.SetConnectTimeout(cfg.ConnectTimeout)
	opts.SetOnConnectHandler(broker.onConnect)
	opts.SetConnectionLostHandler(broker.onConnectionLost)

	broker.client = mqtt.NewClient(opts)
	broker.router = NewRouter(logger)
	broker.publisher = NewPublisher(broker.client, logger)
	broker.subscriber = NewSubscriber(broker.client, broker.router, logger)

	return broker
}

func (b *BrokerImpl) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		return fmt.Errorf("broker already started")
	}

	token := b.client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("connection timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	b.started = true
	b.logger.Info().Msg("MQTT Broker connected successfully")

	return nil
}

func (b *BrokerImpl) Stop(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.started {
		return nil
	}

	b.logger.Info().Msg("Stopping MQTT Broker...")

	b.cancel()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.logger.Info().Msg("All handlers stopped")
	case <-ctx.Done():
		b.logger.Warn().Msg("Stop timeout")
	}

	b.client.Disconnect(250)
	b.started = false

	return nil
}

func (b *BrokerImpl) GetPublisher() *PublisherImpl {
	return b.publisher
}

func (b *BrokerImpl) GetSubscriber() *SubscriberImpl {
	return b.subscriber
}

func (b *BrokerImpl) GetRouter() *RouterImpl {
	return b.router
}

func (b *BrokerImpl) onConnect(client mqtt.Client) {
	b.logger.Info().Msg("Connected to MQTT broker")
}

func (b *BrokerImpl) onConnectionLost(client mqtt.Client, err error) {
	b.logger.Error().Err(err).Msg("Connection to MQTT broker lost")
}
