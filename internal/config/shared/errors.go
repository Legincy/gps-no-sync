package shared

import "fmt"

type ConfigError struct {
	Component string
	Field     string
	Value     interface{}
	Message   string
}

func (e *ConfigError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("%s.%s: %s (got: %v)", e.Component, e.Field, e.Message, e.Value)
	}
	return fmt.Sprintf("%s.%s: %s", e.Component, e.Field, e.Message)
}
