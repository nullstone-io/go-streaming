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
	done         chan struct{}
	closeOnce    sync.Once
	mu           sync.Mutex
	closed       bool
}

func NewBufferedChannelAdapter() *BufferedChannelAdapter {
	adapter := BufferedChannelAdapter{
		messages: make(chan stream.Message),
		buffer:   bytes.NewBufferString(""),
		ticker:   time.NewTicker(bufferTimeout),
		done:     make(chan struct{}),
	}

	go adapter.flushOnTick()

	return &adapter
}

func (a *BufferedChannelAdapter) Channel() <-chan stream.Message {
	return a.messages
}

func (a *BufferedChannelAdapter) Send(message stream.Message) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}

	// First check to see if this message belongs to a different workflow phase
	// If it does, flush all previous messages
	if a.currentPhase != "" && a.currentPhase != message.Context {
		a.flushLocked(false)
	}
	a.currentPhase = message.Context

	a.buffer.WriteString(message.Content)

	// If we have exceeded the min buffer length, flush the content to the stream
	if a.buffer.Len() > maxBufferLength {
		a.flushLocked(false)
	}
}

func (a *BufferedChannelAdapter) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.flushLocked(false)
}

// flushLocked emits the buffered content to the channel; the caller must hold mu.
// If the consumer has stopped reading (e.g. the websocket client disconnected),
// an unguarded channel send would block forever while holding the mutex,
// deadlocking Close(). done unblocks the send so Close() can proceed.
// The final flush during Close() runs with done already closed, so it uses a
// bounded timer instead: a live consumer still receives the tail, a dead one
// only delays Close() by the timeout.
func (a *BufferedChannelAdapter) flushLocked(closing bool) {
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

	if closing {
		t := time.NewTimer(bufferTimeout)
		defer t.Stop()
		select {
		case a.messages <- m:
		case <-t.C:
		}
		return
	}
	select {
	case a.messages <- m:
	case <-a.done:
	}
}

func (a *BufferedChannelAdapter) flushOnTick() {
	for {
		select {
		case <-a.ticker.C:
			a.Flush()
		case <-a.done:
			return
		}
	}
}

func (a *BufferedChannelAdapter) Close() {
	// Signal done before acquiring the mutex; a Send/Flush blocked on the channel
	// holds the mutex and needs done to release it. This also stops flushOnTick.
	a.closeOnce.Do(func() { close(a.done) })
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		// Best-effort delivery of any remaining buffered content
		a.flushLocked(true)
		close(a.messages)
		a.ticker.Stop()
	}
	a.closed = true
}
