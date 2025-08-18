package interfaces

import (
	"context"
	"encoding/json"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"time"
)

type IMqClient interface {
	PublishJson(topic string, data interface{}) error
	Subscribe(topic string, handler mqtt.MessageHandler) error
	Disconnect(ctx context.Context)
	Connect(ctx context.Context) error
}

type ITopicManager interface {
	GetClusterTopic() string
	GetMeasurementTopic() string
	GetStationTopic() string
	GetBaseTopic() string
	ExtractIdFromTopic(topic, template string) (string, error)
	ExtractStationId(topic string) (string, error)
	ExtractClusterId(topic string) (string, error)
}

type ITableListener interface {
	GetTableName() string
	HandleChange(ctx context.Context, event *TableChangeEvent) error
	GetChannelName() string
}

type IListenerManager interface {
	RegisterListener(listener ITableListener) error
	Initialize() error
	Start()
	Stop()
}

type OperationType string

const (
	InsertOperation OperationType = "INSERT"
	UpdateOperation OperationType = "UPDATE"
	DeleteOperation OperationType = "DELETE"
)

type IPqTableListener interface {
	HandleChange(ctx context.Context, event *TableChangeEvent) error
}

type TableChangeEvent struct {
	Operation OperationType          `json:"operation"`
	Table     string                 `json:"table"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
	NewData   map[string]interface{} `json:"new_data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

func (t *TableChangeEvent) GetData() ([]byte, []byte, error) {
	newData, err := json.Marshal(t.NewData)
	if err != nil {
		return nil, nil, err
	}

	oldData, err := json.Marshal(t.OldData)
	if err != nil {
		return nil, nil, err
	}

	return newData, oldData, nil
}
