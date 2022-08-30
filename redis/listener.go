package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/stream"
	"time"
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
		cursor = "1"
	} else {
		cursor = *c
	}

	// since we are no longer able to use the special $ id,
	// we have to wait for the stream to be created before we can start listening
	for {
		result := r.redisClient.Exists(ctx, r.streamName)
		if result.Val() == 1 {
			break
		}
		time.Sleep(time.Second)
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
