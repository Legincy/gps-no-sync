package mqtt

import (
	"fmt"
	"time"
)

type Action string

const (
	ActionRegister Action = "REGISTER"
	ActionUpdate   Action = "UPDATE"
	ActionDelete   Action = "DELETE"
	ActionSync     Action = "SYNC"
	ActionAck      Action = "ACK"
)

type IncomingMessage struct {
	Action Action                 `json:"action"`
	Source string                 `json:"source"`
	Data   map[string]interface{} `json:"data"`
}

type OutgoingMessage struct {
	Source    string      `json:"source"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

func (im *IncomingMessage) Validate() error {
	if im.Source == "" {
		return fmt.Errorf("source is required")
	}
	return nil
}
