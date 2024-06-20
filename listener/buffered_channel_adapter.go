package listener

import (
	"bytes"
	"github.com/nullstone-io/go-streaming/stream"
	"sync"
	"time"
)

const (
	maxBufferLength = 1024
	bufferTimeout   = time.Second
)

type BufferedChannelAdapter struct {
	messages     chan stream.Message
	buffer       *bytes.Buffer
	currentPhase string
	ticker       *time.Ticker
	mu           sync.Mutex
	closed       bool
}

func NewBufferedChannelAdapter() *BufferedChannelAdapter {
	adapter := BufferedChannelAdapter{
		messages: make(chan stream.Message),
		buffer:   bytes.NewBufferString(""),
		ticker:   time.NewTicker(bufferTimeout),
	}

	go adapter.flushOnTick()

	return &adapter
}

func (a *BufferedChannelAdapter) Channel() <-chan stream.Message {
	return a.messages
}

func (a *BufferedChannelAdapter) Send(message stream.Message) {
	// First check to see if this message belongs to a different workflow phase
	// If it does, flush all previous messages

	if a.currentPhase != "" && a.currentPhase != message.Context {
		a.Flush()
	}
	a.currentPhase = message.Context

	a.buffer.WriteString(message.Content)

	// If we have exceeded the min buffer length, flush the content to the stream
	if a.buffer.Len() > maxBufferLength {
		a.Flush()
	}
}

func (a *BufferedChannelAdapter) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}

	a.ticker.Reset(bufferTimeout)

	// Do not send a message if there is no buffered content
	if a.buffer.Len() <= 0 {
		return
	}

	// Dump the buffered content into a message
	// Reset the buffer before emitting to the stream
	m := stream.Message{
		Context: a.currentPhase,
		Content: a.buffer.String(),
	}
	a.buffer.Reset()

	a.messages <- m
}

func (a *BufferedChannelAdapter) flushOnTick() {
	for range a.ticker.C {
		a.Flush()
	}
}

func (a *BufferedChannelAdapter) Close() {
	a.Flush()
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		close(a.messages)
		a.ticker.Stop()
	}
	a.closed = true
}
