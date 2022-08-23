package redis

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/websocket"
)

type ObjectAdapter struct {
	streamName string
	messages   chan<- websocket.Message
}

func NewObjectAdapter(streamName string, messages chan<- websocket.Message) *ObjectAdapter {
	return &ObjectAdapter{
		streamName: streamName,
		messages:   messages,
	}
}

func (o *ObjectAdapter) Send(messages []redis.XMessage) {
	for _, message := range messages {
		m := websocket.Message{
			Source:  o.streamName,
			Context: fmt.Sprintf("%s", message.Values["event"]),
			Content: fmt.Sprintf("%s", message.Values["data"]),
		}
		o.messages <- m
	}
}
