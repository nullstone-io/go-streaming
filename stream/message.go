package stream

import "encoding/json"

const (
	EndOfTransmission = "\x04"
)

type EventType string

const (
	EventTypeCreated EventType = "created"
	EventTypeUpdated EventType = "updated"
	EventTypeDeleted EventType = "deleted"
)

type Message struct {
	Type    string         `json:"type"`
	Context string         `json:"context"`
	Content string         `json:"content"`
	Details map[string]any `json:"details,omitempty"`
}

func (m *Message) ToMap() map[string]interface{} {
	serialized, _ := json.Marshal(m)
	var result map[string]interface{}
	_ = json.Unmarshal(serialized, &result)
	return result
}

func EotMessage() Message {
	return Message{
		Context: "eot",
		Content: EndOfTransmission,
	}
}
