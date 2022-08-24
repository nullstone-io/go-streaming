package listener

import (
	"context"
	"github.com/nullstone-io/go-streaming/stream"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestBufferedChannelAdapter(t *testing.T) {
	tests := []struct {
		name string
		msgs []stream.Message
		want []stream.Message
	}{
		{
			name: "send multiple messages from one phase",
			msgs: []stream.Message{
				{
					Context: "phase-1",
					Content: "this is text\n",
				},
				{
					Context: "phase-1",
					Content: "this is more\n",
				},
				{
					Context: "phase-1",
					Content: "final",
				},
			},
			want: []stream.Message{
				{
					Context: "phase-1",
					Content: "this is text\nthis is more\nfinal",
				},
			},
		},
		{
			name: "send messages from different phases",
			msgs: []stream.Message{
				{
					Context: "phase-1",
					Content: "this is text\n",
				},
				{
					Context: "phase-2",
					Content: "this is more\n",
				},
				{
					Context: "phase-3",
					Content: "this is final\n",
				},
			},
			want: []stream.Message{
				{
					Context: "phase-1",
					Content: "this is text\n",
				},
				{
					Context: "phase-2",
					Content: "this is more\n",
				},
				{
					Context: "phase-3",
					Content: "this is final\n",
				},
			},
		},
		{
			name: "send messages greater than the buffer threshold",
			msgs: []stream.Message{
				{
					Context: "phase-1",
					Content: strings.Repeat("a", 1025),
				},
				{
					Context: "phase-1",
					Content: strings.Repeat("b", 30),
				},
			},
			want: []stream.Message{
				{
					Context: "phase-1",
					Content: strings.Repeat("a", 1025),
				},
				{
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

			gotMsgs := make([]stream.Message, 0)
			messages := make(chan stream.Message)

			go func() {
				defer close(messages)
				adapter := NewBufferedChannelAdapter(messages)
				for _, msg := range test.msgs {
					adapter.Send(msg)
				}
				adapter.Flush()
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
