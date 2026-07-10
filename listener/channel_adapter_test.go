package listener

import (
	"strings"
	"testing"
	"time"

	"github.com/nullstone-io/go-streaming/stream"
	"github.com/stretchr/testify/assert"
)

// runWithTimeout fails the test if fn does not return within the timeout.
// This guards the deadlock regression tests below from hanging the suite.
func runWithTimeout(t *testing.T, timeout time.Duration, name string, fn func()) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fn()
	}()
	select {
	case <-finished:
	case <-time.After(timeout):
		t.Fatalf("%s did not return within %s (deadlock)", name, timeout)
	}
}

func TestChannelAdapter_CloseUnblocksSendWithNoConsumer(t *testing.T) {
	adapter := NewChannelAdapter()

	// Simulate the websocket broker's writeLoop exiting (client disconnected):
	// nobody reads from adapter.Channel(), so Send blocks on the unbuffered channel
	sendReturned := make(chan struct{})
	go func() {
		defer close(sendReturned)
		adapter.Send(stream.Message{Context: "updated", Content: "orphaned"})
	}()

	// Give Send time to park on the channel send while holding the mutex
	time.Sleep(50 * time.Millisecond)

	runWithTimeout(t, 2*time.Second, "Close", adapter.Close)

	select {
	case <-sendReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after Close (leaked goroutine)")
	}
}

func TestChannelAdapter_SendAfterCloseIsNoop(t *testing.T) {
	adapter := NewChannelAdapter()
	adapter.Close()
	runWithTimeout(t, 2*time.Second, "Send", func() {
		adapter.Send(stream.Message{Context: "updated", Content: "after close"})
	})
}

func TestChannelAdapter_CloseIsIdempotent(t *testing.T) {
	adapter := NewChannelAdapter()
	adapter.Close()
	runWithTimeout(t, 2*time.Second, "second Close", adapter.Close)
}

func TestChannelAdapter_DeliversToConsumer(t *testing.T) {
	adapter := NewChannelAdapter()
	defer adapter.Close()

	received := make(chan stream.Message, 1)
	go func() {
		received <- <-adapter.Channel()
	}()

	adapter.Send(stream.Message{Context: "updated", Content: "hello"})

	select {
	case m := <-received:
		assert.Equal(t, "updated", m.Context)
		assert.Equal(t, "hello", m.Content)
	case <-time.After(2 * time.Second):
		t.Fatal("message was not delivered to consumer")
	}
}

func TestBufferedChannelAdapter_CloseUnblocksSendWithNoConsumer(t *testing.T) {
	adapter := NewBufferedChannelAdapter()

	// Exceed maxBufferLength so Send flushes and parks on the channel send
	// with no consumer reading
	sendReturned := make(chan struct{})
	go func() {
		defer close(sendReturned)
		adapter.Send(stream.Message{Context: "phase1", Content: strings.Repeat("x", maxBufferLength+1)})
	}()

	time.Sleep(50 * time.Millisecond)

	runWithTimeout(t, 2*time.Second, "Close", adapter.Close)

	select {
	case <-sendReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not return after Close (leaked goroutine)")
	}
}

func TestBufferedChannelAdapter_CloseDeliversTailToLiveConsumer(t *testing.T) {
	adapter := NewBufferedChannelAdapter()

	received := make(chan stream.Message, 1)
	go func() {
		for m := range adapter.Channel() {
			received <- m
		}
	}()

	adapter.Send(stream.Message{Context: "phase1", Content: "tail content"})
	adapter.Close()

	select {
	case m := <-received:
		assert.Equal(t, "phase1", m.Context)
		assert.Equal(t, "tail content", m.Content)
	case <-time.After(2 * time.Second):
		t.Fatal("buffered tail content was not delivered on Close")
	}
}

func TestBufferedChannelAdapter_FlushesOnPhaseChange(t *testing.T) {
	adapter := NewBufferedChannelAdapter()
	defer adapter.Close()

	received := make(chan stream.Message, 2)
	go func() {
		for m := range adapter.Channel() {
			received <- m
		}
	}()

	adapter.Send(stream.Message{Context: "phase1", Content: "first"})
	adapter.Send(stream.Message{Context: "phase2", Content: "second"})

	select {
	case m := <-received:
		assert.Equal(t, "phase1", m.Context)
		assert.Equal(t, "first", m.Content)
	case <-time.After(2 * time.Second):
		t.Fatal("phase1 content was not flushed on phase change")
	}
}
