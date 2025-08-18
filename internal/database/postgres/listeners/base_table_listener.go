package listeners

type BaseTableListener struct {
	tableName   string
	channelName string
}

func NewBaseTableListener(tableName string) *BaseTableListener {
	return &BaseTableListener{
		tableName:   tableName,
		channelName: "table_events",
	}
}

func (b *BaseTableListener) GetTableName() string {
	return b.tableName
}

func (b *BaseTableListener) GetChannelName() string {
	return b.channelName
}
