package components

import (
	"fmt"
	"github.com/joho/godotenv"
	"gps-no-sync/internal/config/shared"
	"gps-no-sync/internal/interfaces"
	"strings"
	"time"
)

type MQTTConfig interface {
	interfaces.Config
	GetUrl() string
}

type MQTTConfigImpl struct {
	Host                 string        `json:"host"`
	Port                 int           `json:"port"`
	Username             string        `json:"username"`
	Password             string        `json:"password"`
	ClientID             string        `json:"client_id"`
	BaseTopic            string        `json:"base_topic"`
	QoS                  byte          `json:"qos"`
	KeepAlive            time.Duration `json:"keep_alive"`
	AutoReconnect        bool          `json:"auto_reconnect"`
	MaxReconnectInterval time.Duration `json:"max_reconnect_interval"`
	CleanSession         bool          `json:"clean_session"`
}

func NewMQTTConfig() MQTTConfigImpl {
	config := MQTTConfigImpl{}
	config.Load()
	config.SetDefaults()
	return config
}

func (M *MQTTConfigImpl) Load() {
	_ = godotenv.Load()

	M.Host = shared.GetEnv("MQTT_HOST")
	M.Port = shared.GetEnvAsInt("MQTT_PORT")
	M.Username = shared.GetEnv("MQTT_USERNAME")
	M.Password = shared.GetEnv("MQTT_PASSWORD")
	M.ClientID = shared.GetEnv("MQTT_CLIENT_ID")
	M.BaseTopic = shared.GetEnv("MQTT_BASE_TOPIC")
	M.QoS = byte(shared.GetEnvAsInt("MQTT_QOS"))
	M.KeepAlive = shared.GetEnvAsDuration("MQTT_KEEP_ALIVE")
	M.AutoReconnect = shared.GetEnvAsBool("MQTT_AUTO_RECONNECT", true)
	M.MaxReconnectInterval = shared.GetEnvAsDuration("MQTT_MAX_RECONNECT_INTERVAL")
	M.CleanSession = shared.GetEnvAsBool("MQTT_CLEAN_SESSION", true)
}

func (M *MQTTConfigImpl) SetDefaults() {
	if M.Host == "" {
		M.Host = "localhost"
	}
	if M.Port == 0 {
		M.Port = 1883
	}
	if M.ClientID == "" {
		M.ClientID = "gps-no-sync"
	}
	if M.BaseTopic == "" {
		M.BaseTopic = "gps-no"
	}
	if M.QoS <= 0 {
		M.QoS = 1
	}
	if M.KeepAlive == 0 {
		M.KeepAlive = 60 * time.Second
	}
	if M.MaxReconnectInterval == 0 {
		M.MaxReconnectInterval = 10 * time.Second
	}

	M.BaseTopic = strings.TrimSuffix(M.BaseTopic, "/")
}

func (M *MQTTConfigImpl) Validate() error {
	if M.Host == "" {
		return fmt.Errorf("MQTT host is required")
	}

	if M.Port <= 0 || M.Port > 65535 {
		return fmt.Errorf("MQTT port must be between 1 and 65535, got %d", M.Port)
	}

	if M.QoS > 2 {
		return fmt.Errorf("MQTT QoS must be 0, 1, or 2, got %d", M.QoS)
	}

	if M.KeepAlive < 0 {
		return fmt.Errorf("MQTT keep alive cannot be negative, got %d", M.KeepAlive)
	}

	return nil
}

func (M *MQTTConfigImpl) GetUrl() string {
	return fmt.Sprintf("%s:%d", M.Host, M.Port)
}

var _ MQTTConfig = (*MQTTConfigImpl)(nil)
