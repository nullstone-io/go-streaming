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
	msgs         chan<- stream.Message
	buffer       *bytes.Buffer
	currentPhase string
	ticker       *time.Ticker
	mu           sync.Mutex
	done         chan bool
}

func NewBufferedChannelAdapter(messages chan<- stream.Message) *BufferedChannelAdapter {
	adapter := BufferedChannelAdapter{
		msgs:   messages,
		buffer: bytes.NewBufferString(""),
		ticker: time.NewTicker(bufferTimeout),
		done:   make(chan bool),
	}

	go adapter.waitForTick()

	return &adapter
}

func (l *BufferedChannelAdapter) Send(message stream.Message) {
	// First check to see if this message belongs to a different workflow phase
	// If it does, flush all previous messages

	if l.currentPhase != "" && l.currentPhase != message.Context {
		l.Flush()
	}
	l.currentPhase = message.Context

	l.buffer.WriteString(message.Content)

	// If we have exceeded the min buffer length, flush the content to the stream
	if l.buffer.Len() > maxBufferLength {
		l.Flush()
	}
}

func (l *BufferedChannelAdapter) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.ticker.Reset(bufferTimeout)

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

func (l *BufferedChannelAdapter) waitForTick() {
	for {
		select {
		case <-l.done:
			l.ticker.Stop()
			return
		case <-l.ticker.C:
			l.Flush()
		}
	}
}

func (l *BufferedChannelAdapter) Close() {
	close(l.done)
}
