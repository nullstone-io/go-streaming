package redis

import (
	"bytes"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/websocket"
)

const (
	maxBufferLength = 1024
)

type LogAdapter struct {
	streamName   string
	msgs         chan<- websocket.Message
	buffer       *bytes.Buffer
	currentPhase string
}

func NewLogAdapter(streamName string, messages chan<- websocket.Message) *LogAdapter {
	return &LogAdapter{
		streamName: streamName,
		msgs:       messages,
	}
}

func (l *LogAdapter) Send(messages []redis.XMessage) {
	l.buffer = bytes.NewBufferString("")

	for _, msg := range messages {
		phase := fmt.Sprintf("%s", msg.Values["phase"])
		content := fmt.Sprintf("%s", msg.Values["line"])

		// First check to see if this message belongs to a different workflow phase
		// If it does, flush all previous messages
		if l.currentPhase != "" && l.currentPhase != phase {
			l.flush()
		}
		l.currentPhase = phase

		l.buffer.WriteString(content)

		// If we have exceeded the min buffer length, flush the content to the stream
		if l.buffer.Len() > maxBufferLength {
			l.flush()
		}
	}

	l.flush()
}

func (l *LogAdapter) flush() {
	// Do not send a message if there is no buffered content
	if l.buffer.Len() <= 0 {
		return
	}

	// Dump the buffered content into a message
	// Reset the buffer before emitting to the stream
	m := websocket.Message{
		Source:  l.streamName,
		Context: l.currentPhase,
		Content: l.buffer.String(),
	}
	l.buffer.Reset()

	l.msgs <- m
}
