package listeners

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gps-no-sync/internal/config"
	"time"
)

type ListenerManager struct {
	db        *gorm.DB
	listener  *pq.Listener
	logger    zerolog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	listeners map[string][]TableListener
	channels  map[string]bool
}

func NewListenerManager(db *gorm.DB, cfg *config.PostgresConfig, logger zerolog.Logger) *ListenerManager {
	ctx, cancel := context.WithCancel(context.Background())

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database,
		func() string {
			if cfg.SSLMode == "false" || cfg.SSLMode == "" {
				return "disable"
			}
			return cfg.SSLMode
		}(),
	)

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logger.Error().Err(err).Msg("PostgreSQL listener error")
		}
	}

	listener := pq.NewListener(dsn, 10*time.Second, time.Minute, reportProblem)

	return &ListenerManager{
		db:        db,
		listener:  listener,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		listeners: make(map[string][]TableListener),
		channels:  make(map[string]bool),
	}
}

func (lm *ListenerManager) RegisterListener(listener TableListener) error {
	tableName := listener.GetTableName()
	channelName := listener.GetChannelName()

	lm.listeners[tableName] = append(lm.listeners[tableName], listener)

	if !lm.channels[channelName] {
		if err := lm.listener.Listen(channelName); err != nil {
			return fmt.Errorf("failed to listen on channel %s: %w", channelName, err)
		}
		lm.channels[channelName] = true
		lm.logger.Info().Str("channel", channelName).Msg("Registered PostgreSQL listener channel")
	}

	lm.logger.Info().
		Str("table", tableName).
		Str("channel", channelName).
		Msg("Registered table listener")

	return nil
}

func (lm *ListenerManager) Initialize() error {
	if err := lm.setupTriggers(); err != nil {
		return fmt.Errorf("failed to setup triggers: %w", err)
	}

	lm.logger.Info().Msg("Listener manager initialized")
	return nil
}

func (lm *ListenerManager) setupTriggers() error {
	createFunctionSQL := `
	CREATE OR REPLACE FUNCTION notify_table_change() RETURNS trigger AS $$
	DECLARE
		notification json;
		old_data json := NULL;
		new_data json := NULL;
	BEGIN
		IF TG_OP = 'DELETE' THEN
			old_data = row_to_json(OLD);
		ELSIF TG_OP = 'INSERT' THEN
			new_data = row_to_json(NEW);
		ELSIF TG_OP = 'UPDATE' THEN
			old_data = row_to_json(OLD);
			new_data = row_to_json(NEW);
		END IF;
		
		notification = json_build_object(
			'operation', TG_OP,
			'table', TG_TABLE_NAME,
			'old_data', old_data,
			'new_data', new_data,
			'timestamp', now()
		);
		
		PERFORM pg_notify('table_changes', notification::text);
		
		IF TG_OP = 'DELETE' THEN
			RETURN OLD;
		ELSE
			RETURN NEW;
		END IF;
	END;
	$$ LANGUAGE plpgsql;`

	if err := lm.db.Exec(createFunctionSQL).Error; err != nil {
		return fmt.Errorf("failed to create notify function: %w", err)
	}

	for tableName := range lm.listeners {
		if err := lm.createTriggerForTable(tableName); err != nil {
			return fmt.Errorf("failed to create trigger for table %s: %w", tableName, err)
		}
	}

	return nil
}

func (lm *ListenerManager) createTriggerForTable(tableName string) error {
	triggerSQL := fmt.Sprintf(`
	DROP TRIGGER IF EXISTS %s_change_trigger ON %s;
	CREATE TRIGGER %s_change_trigger
		AFTER INSERT OR UPDATE OR DELETE ON %s
		FOR EACH ROW EXECUTE FUNCTION notify_table_change();`,
		tableName, tableName, tableName, tableName)

	return lm.db.Exec(triggerSQL).Error
}

func (lm *ListenerManager) listenForChanges() {
	for {
		select {
		case notification := <-lm.listener.Notify:
			if notification != nil {
				lm.handleNotification(notification.Extra)
			}
		case <-time.After(90 * time.Second):
			if err := lm.listener.Ping(); err != nil {
				lm.logger.Error().Err(err).Msg("PostgreSQL listener ping failed")
				return
			}
		case <-lm.ctx.Done():
			lm.logger.Info().Msg("Table listener manager stopping...")
			return
		}
	}
}

func (lm *ListenerManager) handleNotification(payload string) {
	var event TableChangeEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		lm.logger.Error().Err(err).Str("payload", payload).Msg("Failed to parse notification")
		return
	}

	tableListeners, exists := lm.listeners[event.Table]
	if !exists {
		lm.logger.Debug().Str("table", event.Table).Msg("No listeners registered for table")
		return
	}

	for _, listener := range tableListeners {
		go func(l TableListener) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := l.HandleChange(ctx, &event); err != nil {
				lm.logger.Error().Err(err).
					Str("table", event.Table).
					Str("listener", fmt.Sprintf("%T", l)).
					Msg("Error handling table change")
			}
		}(listener)
	}
}

func (lm *ListenerManager) Start() {
	lm.logger.Info().Msg("🎧 Starting table listener manager...")
	go lm.listenForChanges()
}

func (lm *ListenerManager) Stop() {
	if lm != nil {
		lm.cancel()
		lm.logger.Info().Msg("Table listener manager stopped")
	}
}
