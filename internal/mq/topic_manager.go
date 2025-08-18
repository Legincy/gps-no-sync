package mq

import (
	"fmt"
	"github.com/rs/zerolog"
	"regexp"
	"strings"
)

type TopicManager struct {
	BaseTopic string
	logger    zerolog.Logger
}

func NewTopicManager(baseTopic string, logger zerolog.Logger) *TopicManager {
	return &TopicManager{
		BaseTopic: baseTopic,
		logger:    logger,
	}
}

const (
	StationTopicTemplate     = "%s/v1/stations/+"
	MeasurementTopicTemplate = "%s/v1/measurements/+"
	ClusterTopicTemplate     = "%s/v1/clusters/+"
)

func (m *TopicManager) GetStationTopic() string {
	return fmt.Sprintf(StationTopicTemplate, m.BaseTopic)
}

func (m *TopicManager) GetMeasurementTopic() string {
	return fmt.Sprintf(MeasurementTopicTemplate, m.BaseTopic)
}

func (m *TopicManager) GetClusterTopic() string {
	return fmt.Sprintf(ClusterTopicTemplate, m.BaseTopic)
}

func (m *TopicManager) buildTopicRegex(template string) *regexp.Regexp {
	pattern := strings.ReplaceAll(template, "%s", m.BaseTopic)
	pattern = strings.ReplaceAll(pattern, "+", "([^/]+)")
	pattern = "^" + pattern + "$"

	return regexp.MustCompile(pattern)
}

func (m *TopicManager) ExtractIdFromTopic(topic, template string) (string, error) {
	regex := m.buildTopicRegex(template)
	matches := regex.FindStringSubmatch(topic)

	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract ID from topic: %s", topic)
	}

	return matches[1], nil
}

func (m *TopicManager) ExtractStationId(topic string) (string, error) {
	return m.ExtractIdFromTopic(topic, StationTopicTemplate)
}

func (m *TopicManager) ExtractClusterId(topic string) (string, error) {
	return m.ExtractIdFromTopic(topic, ClusterTopicTemplate)
}

func (m *TopicManager) GetBaseTopic() string {
	if strings.HasSuffix(m.BaseTopic, "/") {
		return m.BaseTopic[:len(m.BaseTopic)-1]
	}
	return m.BaseTopic
}
