package listener

import (
	"github.com/nullstone-io/go-streaming/stream"
	"sync"
)

type ChannelAdapter struct {
	messages chan stream.Message
	mu       sync.Mutex
	closed   bool
}

func NewChannelAdapter() *ChannelAdapter {
	return &ChannelAdapter{
		messages: make(chan stream.Message),
	}
}

func (a *ChannelAdapter) Channel() <-chan stream.Message {
	return a.messages
}

func (a *ChannelAdapter) Send(message stream.Message) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}

	a.messages <- message
}

func (a *ChannelAdapter) Flush() {
	// Do nothing
}

func (a *ChannelAdapter) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		close(a.messages)
	}
	a.closed = true
}
