package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/stream"
)

type Adapter interface {
	Send(message stream.Message)
	Flush()
}

type Listener struct {
	streamName  string
	redisClient *redis.Client
	adapter     Adapter
}

func NewListener(redisClient *redis.Client, streamName string, adapter Adapter) *Listener {
	return &Listener{
		streamName:  streamName,
		redisClient: redisClient,
		adapter:     adapter,
	}
}

func (r *Listener) Listen(ctx context.Context, c *string) error {
	var cursor string
	if c == nil {
		cursor = "$" // the special $ id will return only items added after we block on XREAD
	} else {
		cursor = *c
	}

	for {
		args := redis.XReadArgs{
			Streams: []string{r.streamName, cursor},
			Block:   0,
		}
		groups, err := r.redisClient.XRead(ctx, &args).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			} else {
				return err
			}
		}
		for _, grp := range groups {
			for _, msg := range grp.Messages {
				m := stream.Message{
					Context: msg.Values["context"].(string),
					Content: msg.Values["content"].(string),
				}
				r.adapter.Send(m)
				cursor = msg.ID
			}
		}
		r.adapter.Flush()
	}
}
