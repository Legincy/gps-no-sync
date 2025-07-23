package mqtt

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
	DeviceTopicTemplate  = "%s/+/device/raw"
	RangingTopicTemplate = "%s/+/uwb/ranging"
)

func (m *TopicManager) BuildDeviceTopicSubscription() string {
	return fmt.Sprintf(DeviceTopicTemplate, m.BaseTopic)
}

func (m *TopicManager) BuildRangingTopicSubscription() string {
	return fmt.Sprintf(RangingTopicTemplate, m.BaseTopic)
}

func (m *TopicManager) BuildTopicRegex(template string) *regexp.Regexp {
	pattern := strings.ReplaceAll(template, "%s", m.BaseTopic)
	pattern = strings.ReplaceAll(pattern, "+", "([^/]+)")
	pattern = "^" + pattern + "$"

	return regexp.MustCompile(pattern)
}

func (m *TopicManager) ExtractDeviceIDFromTopic(topic, template string) (string, error) {
	regex := m.BuildTopicRegex(template)
	matches := regex.FindStringSubmatch(topic)

	fmt.Printf("%+v\n", matches)

	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract device ID from topic: %s", topic)
	}

	return matches[1], nil
}
