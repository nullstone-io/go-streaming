package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/websocket"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestLogAdapter(t *testing.T) {
	streamName := "stream-1"

	tests := []struct {
		name      string
		redisMsgs []redis.XMessage
		want      []websocket.Message
	}{
		{
			name: "send multiple messages from one phase",
			redisMsgs: []redis.XMessage{
				{
					ID: "0",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  "this is text\n",
					},
				},
				{
					ID: "1",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  "this is more\n",
					},
				},
				{
					ID: "2",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  "final",
					},
				},
			},
			want: []websocket.Message{
				{
					Source:  streamName,
					Context: "phase-1",
					Content: "this is text\nthis is more\nfinal",
				},
			},
		},
		{
			name: "send messages from different phases",
			redisMsgs: []redis.XMessage{
				{
					ID: "0",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  "this is text\n",
					},
				},
				{
					ID: "1",
					Values: map[string]interface{}{
						"phase": "phase-2",
						"line":  "this is more\n",
					},
				},
				{
					ID: "2",
					Values: map[string]interface{}{
						"phase": "phase-3",
						"line":  "this is final\n",
					},
				},
			},
			want: []websocket.Message{
				{
					Source:  streamName,
					Context: "phase-1",
					Content: "this is text\n",
				},
				{
					Source:  streamName,
					Context: "phase-2",
					Content: "this is more\n",
				},
				{
					Source:  streamName,
					Context: "phase-3",
					Content: "this is final\n",
				},
			},
		},
		{
			name: "send messages greater than the buffer threshold",
			redisMsgs: []redis.XMessage{
				{
					ID: "0",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  strings.Repeat("a", 1025),
					},
				},
				{
					ID: "1",
					Values: map[string]interface{}{
						"phase": "phase-1",
						"line":  strings.Repeat("b", 30),
					},
				},
			},
			want: []websocket.Message{
				{
					Source:  streamName,
					Context: "phase-1",
					Content: strings.Repeat("a", 1025),
				},
				{
					Source:  streamName,
					Context: "phase-1",
					Content: strings.Repeat("b", 30),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			gotMsgs := make([]websocket.Message, 0)
			messages := make(chan websocket.Message)

			go func() {
				defer close(messages)
				adapter := NewLogAdapter(streamName, messages)
				adapter.Send(test.redisMsgs)
			}()

			func() {
				for {
					select {
					case message, ok := <-messages:
						if !ok {
							return
						}
						gotMsgs = append(gotMsgs, message)
					case <-ctx.Done():
						return
					}
				}
			}()

			assert.Equal(t, test.want, gotMsgs)
		})
	}
}
