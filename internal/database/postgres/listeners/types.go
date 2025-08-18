package listeners

import (
	"context"
	"time"
)

type OperationType string

const (
	InsertOperation OperationType = "INSERT"
	UpdateOperation OperationType = "UPDATE"
	DeleteOperation OperationType = "DELETE"
)

type TableChangeEvent struct {
	Operation OperationType          `json:"operation"`
	Table     string                 `json:"table"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
	NewData   map[string]interface{} `json:"new_data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

type TableListener interface {
	GetTableName() string
	HandleChange(ctx context.Context, event *TableChangeEvent) error
	GetChannelName() string
}

type BaseTableListener struct {
	tableName   string
	channelName string
}

func NewBaseTableListener(tableName string) *BaseTableListener {
	return &BaseTableListener{
		tableName:   tableName,
		channelName: "table_changes",
	}
}

func (b *BaseTableListener) GetTableName() string {
	return b.tableName
}

func (b *BaseTableListener) GetChannelName() string {
	return b.channelName
}
