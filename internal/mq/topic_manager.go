package mq

import (
	"fmt"
	"regexp"
	"strings"
)

type TopicManager struct {
	BaseTopic string
}

func NewTopicManager(baseTopic string) *TopicManager {
	return &TopicManager{
		BaseTopic: baseTopic,
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

func (m *TopicManager) BuildTopicRegex(template string) *regexp.Regexp {
	pattern := strings.ReplaceAll(template, "%s", m.BaseTopic)
	pattern = strings.ReplaceAll(pattern, "+", "([^/]+)")
	pattern = "^" + pattern + "$"

	return regexp.MustCompile(pattern)
}

func (m *TopicManager) ExtractStationIdFromTopic(topic, template string) (string, error) {
	regex := m.BuildTopicRegex(template)
	matches := regex.FindStringSubmatch(topic)

	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract device ID from topic: %s", topic)
	}

	return matches[1], nil
}

func (m *TopicManager) ExtractStationId(topic string) (string, error) {
	return m.ExtractStationIdFromTopic(topic, StationTopicTemplate)
}

func (m *TopicManager) ExtractClusterIDFromTopic(topic string) (string, error) {
	return m.ExtractStationIdFromTopic(topic, ClusterTopicTemplate)
}

func (m *TopicManager) FormatMacAddress(topicMac string) string {
	if len(topicMac) == 12 {
		return fmt.Sprintf("%s:%s:%s:%s:%s:%s",
			topicMac[0:2], topicMac[2:4], topicMac[4:6],
			topicMac[6:8], topicMac[8:10], topicMac[10:12])
	}
	return topicMac
}
