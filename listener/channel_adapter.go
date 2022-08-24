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

	/*
		for _, message := range messages {
			m := stream.Message{
				Context: fmt.Sprintf("%s", message.Values["event"]),
				Content: fmt.Sprintf("%s", message.Values["data"]),
			}
			c.messages <- m
		}
	*/
}

func (c *ChannelAdapter) Flush() {
	// Do nothing
}
