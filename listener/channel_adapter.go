package listener

import (
	"github.com/nullstone-io/go-streaming/stream"
)

type ChannelAdapter struct {
	messages chan<- stream.Message
}

func NewChannelAdapter(messages chan<- stream.Message) *ChannelAdapter {
	return &ChannelAdapter{
		messages: messages,
	}
}

func (c *ChannelAdapter) Send(message stream.Message) {
	c.messages <- message
}

func (c *ChannelAdapter) Flush() {
	// Do nothing
}
