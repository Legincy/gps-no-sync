package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/config"
	"math/rand"
	"time"
)

type Client struct {
	client    mqtt.Client
	config    *config.MQTTConfig
	logger    zerolog.Logger
	connected bool
}

func NewClient(cfg *config.MQTTConfig, logger zerolog.Logger) (*Client, error) {
	opts := mqtt.NewClientOptions()

	brokerURL := fmt.Sprintf("tcp://%s:%d", cfg.Host, cfg.Port)
	opts.AddBroker(brokerURL)

	clientID := fmt.Sprintf("%s-%d", cfg.ClientID, rand.Intn(10000))
	opts.SetClientID(clientID)

	if cfg.Username != "" && cfg.Password != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	opts.SetKeepAlive(time.Duration(cfg.KeepAlive) * time.Second)
	opts.SetAutoReconnect(cfg.AutoReconnect)
	opts.SetMaxReconnectInterval(cfg.MaxReconnectInterval)
	opts.SetCleanSession(cfg.CleanSession)

	mqttClient := &Client{
		config:    cfg,
		logger:    logger,
		connected: false,
	}

	opts.SetOnConnectHandler(mqttClient.onConnect)
	opts.SetConnectionLostHandler(mqttClient.onConnectionLost)

	mqttClient.client = mqtt.NewClient(opts)

	return mqttClient, nil
}

func (c *Client) Connect(ctx context.Context) error {
	token := c.client.Connect()

	select {
	case <-token.Done():
		if token.Error() != nil {
			return fmt.Errorf("error connecting to MQTT broker: %w", token.Error())
		}
		c.connected = true
		return nil
	case <-ctx.Done():
		return fmt.Errorf("connection to MQTT broker timed out: %w", ctx.Err())
	}
}

func (c *Client) Disconnect() {
	if c.client.IsConnected() {
		c.logger.Info().Msg("disconnecting from MQTT broker...")
		c.client.Disconnect(250)
		c.connected = false
	}
}

func (c *Client) Subscribe(topic string, handler mqtt.MessageHandler) error {
	if !c.client.IsConnected() {
		return fmt.Errorf("MQTT client is not connected, cannot subscribe to topic %s", topic)
	}

	token := c.client.Subscribe(topic, c.config.QoS, handler)
	token.Wait()

	if token.Error() != nil {
		return fmt.Errorf("error subscribing to topic %s: %w", topic, token.Error())
	}

	c.logger.Info().Str("topic", topic).Msg("Added topic subscription")

	return nil
}

func (c *Client) Publish(topic string, payload []byte) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTT client is not connected")
	}

	token := c.client.Publish(topic, 0, false, payload)
	token.Wait()

	if token.Error() != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, token.Error())
	}

	c.logger.Debug().
		Str("topic", topic).
		Int("payload_size", len(payload)).
		Msg("successfully published message")

	return nil
}

func (c *Client) PublishJSON(topic string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	fmt.Printf("Topic: &")
	return c.Publish(topic, payload)
}

func (c *Client) IsConnected() bool {
	return c.connected && c.client.IsConnected()
}

func (c *Client) onConnect(client mqtt.Client) {
	c.connected = true
	c.logger.Info().
		Str("broker", c.config.Host).
		Msg("Successfully connected to broker")
}

func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	c.connected = false
	c.logger.Warn().Err(err).Msg("lost connection to broker")
}
