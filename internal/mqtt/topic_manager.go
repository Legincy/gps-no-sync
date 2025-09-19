package mqtt

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

const (
	StationSubPattern      = "%s/stations/+"
	MeasurementSubPattern  = "%s/measurements/+"
	StationRootPattern     = "%s/stations"
	MeasurementRootPattern = "%s/measurements"
)

type TopicInfo struct {
	Type     string
	ID       string
	IsRoot   bool
	FullPath string
}

type TopicManagerImpl struct {
	baseTopic string
	patterns  map[string]*regexp.Regexp
	mu        sync.RWMutex
}

func NewTopicManager(baseTopic string) *TopicManagerImpl {
	baseTopic = strings.TrimSuffix(baseTopic, "/")

	return &TopicManagerImpl{
		baseTopic: baseTopic,
		patterns:  make(map[string]*regexp.Regexp),
	}
}

func (tm *TopicManagerImpl) GetStationRootTopic() string {
	return fmt.Sprintf(StationRootPattern, tm.baseTopic)
}

func (tm *TopicManagerImpl) GetMeasurementRootTopic() string {
	return fmt.Sprintf(MeasurementRootPattern, tm.baseTopic)
}

func (tm *TopicManagerImpl) GetStationSubTopic() string {
	return fmt.Sprintf(StationSubPattern, tm.baseTopic)
}

func (tm *TopicManagerImpl) GetMeasurementSubTopic() string {
	return fmt.Sprintf(MeasurementSubPattern, tm.baseTopic)
}

func (tm *TopicManagerImpl) ExtractStationID(topic string) (string, error) {
	return tm.extractID(topic, StationSubPattern)
}

func (tm *TopicManagerImpl) ExtractMeasurementID(topic string) (string, error) {
	return tm.extractID(topic, MeasurementSubPattern)
}

func (tm *TopicManagerImpl) IsStationTopic(topic string) bool {
	return strings.HasPrefix(topic, fmt.Sprintf(StationRootPattern, tm.baseTopic))
}

func (tm *TopicManagerImpl) IsMeasurementTopic(topic string) bool {
	return strings.HasPrefix(topic, fmt.Sprintf(MeasurementRootPattern, tm.baseTopic))
}

func (tm *TopicManagerImpl) IsRootTopic(topic string) bool {
	parts := strings.Split(strings.TrimPrefix(topic, tm.baseTopic+"/"), "/")
	return len(parts) == 1
}

func (tm *TopicManagerImpl) extractID(topic, pattern string) (string, error) {
	regex := tm.getOrCreateRegex(pattern)
	matches := regex.FindStringSubmatch(topic)

	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract ID from topic '%s'", topic)
	}

	return matches[1], nil
}

func (tm *TopicManagerImpl) getOrCreateRegex(pattern string) *regexp.Regexp {
	tm.mu.RLock()
	if regex, exists := tm.patterns[pattern]; exists {
		tm.mu.RUnlock()
		return regex
	}
	tm.mu.RUnlock()

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if regex, exists := tm.patterns[pattern]; exists {
		return regex
	}

	regex := tm.buildTopicRegex(pattern)
	tm.patterns[pattern] = regex
	return regex
}

func (tm *TopicManagerImpl) buildTopicRegex(pattern string) *regexp.Regexp {
	regexPattern := strings.ReplaceAll(pattern, "%s", regexp.QuoteMeta(tm.baseTopic))
	regexPattern = strings.ReplaceAll(regexPattern, "+", "([^/]+)")
	regexPattern = strings.ReplaceAll(regexPattern, "#", "(.*)")
	regexPattern = "^" + regexPattern + "$"

	return regexp.MustCompile(regexPattern)
}

func (tm *TopicManagerImpl) ValidateTopic(topic string) error {
	if !strings.HasPrefix(topic, tm.baseTopic) {
		return fmt.Errorf("topic '%s' doesn't start with base topic '%s'", topic, tm.baseTopic)
	}

	if !tm.IsStationTopic(topic) &&
		!tm.IsMeasurementTopic(topic) {
		return fmt.Errorf("unknown topic type: %s", topic)
	}

	return nil
}

func (tm *TopicManagerImpl) ParseTopic(topic string) (*TopicInfo, error) {
	if err := tm.ValidateTopic(topic); err != nil {
		return nil, err
	}

	info := &TopicInfo{
		FullPath: topic,
		IsRoot:   tm.IsRootTopic(topic),
	}

	switch {
	case tm.IsStationTopic(topic):
		info.Type = "station"
		if !info.IsRoot {
			id, _ := tm.ExtractStationID(topic)
			info.ID = id
		}
	case tm.IsMeasurementTopic(topic):
		info.Type = "measurement"
		if !info.IsRoot {
			id, _ := tm.ExtractMeasurementID(topic)
			info.ID = id
		}
	}

	return info, nil
}

func (tm *TopicManagerImpl) GetBaseTopic() string {
	return tm.baseTopic
}
