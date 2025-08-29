package mq

import (
	"context"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"gps-no-sync/internal/config/components"
	"math/rand"
	"time"
)

type Message struct {
	Data   interface{} `json:"data"`
	Source string      `json:"source"`
}

type MessageOptions struct {
	Qos      byte          `json:"qos"`
	Retained bool          `json:"retained"`
	Timeout  time.Duration `json:"timeout"`
	Source   string        `json:"source"`
}

func DefaultMessageOptions() *MessageOptions {
	return &MessageOptions{
		Qos:      0,
		Retained: true,
		Timeout:  5 * time.Second,
		Source:   "SYNC",
	}
}

type Client struct {
	client    mqtt.Client
	config    components.MQTTConfig
	logger    zerolog.Logger
	connected bool
}

func NewClient(brokerUrl, username, password, clientPrefix string, keepAlive, maxReconnect time.Duration, autoReconnect, cleanSession bool, logger zerolog.Logger) (*Client, error) {
	opts := mqtt.NewClientOptions()

	fmt.Println(brokerUrl, clientPrefix, username, password, keepAlive, maxReconnect, autoReconnect, cleanSession)

	opts.AddBroker(brokerUrl)
	clientID := fmt.Sprintf("%s-%d", clientPrefix, rand.Intn(10000))
	opts.SetClientID(clientID)

	if username != "" && password != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	opts.SetKeepAlive(keepAlive)
	opts.SetAutoReconnect(autoReconnect)
	opts.SetMaxReconnectInterval(maxReconnect)
	opts.SetCleanSession(cleanSession)

	mqttClient := &Client{
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
			return fmt.Errorf("error connecting to MQTTConfig broker: %w", token.Error())
		}
		c.connected = true
		return nil
	case <-ctx.Done():
		return fmt.Errorf("connection to MQTTConfig broker timed out: %w", ctx.Err())
	}
}

func (c *Client) Disconnect(ctx context.Context) {
	if !c.IsConnected() {
		c.logger.Warn().Msg("MQTTConfig client is not connected, nothing to disconnect")
		return
	}

	c.client.Disconnect(250)

	select {
	case <-ctx.Done():
		c.logger.Warn().Msg("MQTTConfig client disconnect timed out")
	default:
		c.connected = false
		c.logger.Info().Msg("MQTTConfig client disconnected successfully")
	}
}

func (c *Client) Subscribe(topic string, qos byte, handler mqtt.MessageHandler) error {
	if !c.client.IsConnected() {
		return fmt.Errorf("MQTTConfig client is not connected, cannot subscribe to topic %s", topic)
	}

	token := c.client.Subscribe(topic, qos, handler)
	token.Wait()

	if token.Error() != nil {
		return fmt.Errorf("error subscribing to topic %s: %w", topic, token.Error())
	}

	c.logger.Info().Str("topic", topic).Msg("Added topic subscription")

	return nil
}

func (c *Client) PublishWithOptions(topic string, payload []byte, options *MessageOptions) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTTConfig client is not connected")
	}

	token := c.client.Publish(topic, options.Qos, options.Retained, payload)
	token.WaitTimeout(options.Timeout)

	if token.Error() != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, token.Error())
	}

	c.logger.Debug().
		Str("topic", topic).
		Int("payload_size", len(payload)).
		Msg("successfully published message with options")

	return nil
}

func (c *Client) Publish(topic string, payload []byte) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTTConfig client is not connected")
	}

	msgOptions := DefaultMessageOptions()

	err := c.PublishWithOptions(topic, payload, msgOptions)
	if err != nil {
		return fmt.Errorf("failed to publish message to topic %s: %w", topic, err)
	}

	c.logger.Debug().
		Str("topic", topic).
		Int("payload_size", len(payload)).
		Msg("successfully published message")

	return nil
}

func (c *Client) PublishJson(topic string, data interface{}) error {
	msgOptions := &MessageOptions{
		Qos:      0,
		Retained: true,
		Timeout:  5 * time.Second,
		Source:   "SYNC",
	}

	message := Message{
		Data:   data,
		Source: msgOptions.Source,
	}

	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return c.PublishWithOptions(topic, payload, msgOptions)
}

func (c *Client) IsConnected() bool {
	return c.connected && c.client.IsConnected()
}

func (c *Client) onConnect(client mqtt.Client) {
	c.connected = true

	c.logger.Info().
		Msg("Successfully connected to broker")
}

func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	c.connected = false
	c.logger.Warn().Err(err).Msg("lost connection to broker")
}
