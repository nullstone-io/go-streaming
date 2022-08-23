package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

type Adapter interface {
	Send([]redis.XMessage)
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

func (r *Listener) Listen(ctx context.Context) error {
	var cursor = "$" // the special $ id will return only items added after we block on XREAD

	for {
		args := redis.XReadArgs{
			Streams: []string{r.streamName, cursor},
			Block:   0,
		}
		streams, err := r.redisClient.XRead(ctx, &args).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			} else {
				return err
			}
		}
		for _, stream := range streams {
			r.adapter.Send(stream.Messages)
			cursor = stream.Messages[len(stream.Messages)-1].ID
		}
	}
}
