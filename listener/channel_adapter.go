package listener

import (
	"github.com/nullstone-io/go-streaming/stream"
	"sync"
)

type ChannelAdapter struct {
	messages  chan stream.Message
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	closed    bool
}

func NewChannelAdapter() *ChannelAdapter {
	return &ChannelAdapter{
		messages: make(chan stream.Message),
		done:     make(chan struct{}),
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

	// If the consumer has stopped reading (e.g. the websocket client disconnected),
	// an unguarded channel send would block forever while holding the mutex,
	// deadlocking Close(). done unblocks the send so Close() can proceed.
	select {
	case a.messages <- message:
	case <-a.done:
	}
}

func (a *ChannelAdapter) Flush() {
	// Do nothing
}

func (a *ChannelAdapter) Close() {
	// Signal done before acquiring the mutex; a Send blocked on the channel
	// holds the mutex and needs done to release it
	a.closeOnce.Do(func() { close(a.done) })
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		close(a.messages)
	}
	a.closed = true
}
