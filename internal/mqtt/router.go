package mqtt

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"regexp"
	"sync"
)

type TopicHandler interface {
	Process(ctx context.Context, msg mqtt.Message) error
}

type RouterImpl struct {
	logger   zerolog.Logger
	handlers map[string]TopicHandler
	patterns map[string]*regexp.Regexp
	mu       sync.RWMutex
}

func NewRouter(logger zerolog.Logger) *RouterImpl {
	return &RouterImpl{
		logger:   logger.With().Str("component", "router").Logger(),
		handlers: make(map[string]TopicHandler),
		patterns: make(map[string]*regexp.Regexp),
	}
}

func (r *RouterImpl) RegisterMultipleTopics(topicPatterns []string, handler TopicHandler) {
	for _, topicPattern := range topicPatterns {
		r.RegisterHandler(topicPattern, handler)
	}
}

func (r *RouterImpl) RegisterHandler(topicPattern string, handler TopicHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlers[topicPattern] = handler

	pattern := r.topicToRegex(topicPattern)
	r.patterns[topicPattern] = regexp.MustCompile(pattern)

	r.logger.Info().
		Str("topic_pattern", topicPattern).
		Msg("Handler registered")
}

func (r *RouterImpl) Route(ctx context.Context, topic string, msg mqtt.Message) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for pattern, regex := range r.patterns {
		if regex.MatchString(topic) {
			if handler, ok := r.handlers[pattern]; ok {
				return handler.Process(ctx, msg)
			}
		}
	}

	return fmt.Errorf("no handler found for topic '%s'", topic)
}

func (r *RouterImpl) topicToRegex(topic string) string {
	pattern := topic
	pattern = regexp.QuoteMeta(pattern)
	pattern = "^" + pattern + "$"
	pattern = regexp.MustCompile(`\\\+`).ReplaceAllString(pattern, `[^/]+`)
	pattern = regexp.MustCompile(`\\\#`).ReplaceAllString(pattern, `.*`)

	return pattern
}
