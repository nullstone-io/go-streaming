package listener

import (
	"bytes"
	"github.com/nullstone-io/go-streaming/stream"
	"strings"
)

const (
	maxBufferLength = 1024
)

type BufferedChannelAdapter struct {
	msgs         chan<- stream.Message
	buffer       *bytes.Buffer
	currentPhase string
}

func NewBufferedChannelAdapter(messages chan<- stream.Message) *BufferedChannelAdapter {
	return &BufferedChannelAdapter{
		msgs:   messages,
		buffer: bytes.NewBufferString(""),
	}
}

func (l *BufferedChannelAdapter) Send(message stream.Message) {
	// First check to see if this message belongs to a different workflow phase
	// If it does, flush all previous messages
	if l.currentPhase != "" && l.currentPhase != message.Context {
		l.Flush()
	}
	l.currentPhase = message.Context

	l.buffer.WriteString(message.Content)

	// If an EOT message is received, go ahead and flush the buffer because we won't get another message
	if strings.HasSuffix(message.Content, stream.EndOfTransmission) {
		l.Flush()
	}

	// If we have exceeded the min buffer length, flush the content to the stream
	if l.buffer.Len() > maxBufferLength {
		l.Flush()
	}
}

func (l *BufferedChannelAdapter) Flush() {
	// Do not send a message if there is no buffered content
	if l.buffer.Len() <= 0 {
		return
	}

	// Dump the buffered content into a message
	// Reset the buffer before emitting to the stream
	m := stream.Message{
		Context: l.currentPhase,
		Content: l.buffer.String(),
	}
	l.buffer.Reset()

	l.msgs <- m
}
