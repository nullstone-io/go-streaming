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

func (r *Listener) Listen(ctx context.Context, cursor string) error {
	if cursor == "-1" {
		cursor = "$" // the special id $ allows us to start streaming from this point in time, even if the stream doesn't exist yet
	}

	for {
		groups, err := r.readWithTimeout(ctx, cursor, time.Second)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// this occurs when nothing is being read from the redis stream in the given timeout
				r.adapter.Flush()
				continue
			} else if errors.Is(err, context.Canceled) {
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
	}
}

func (r *Listener) readWithTimeout(parentCtx context.Context, cursor string, timeout time.Duration) ([]redis.XStream, error) {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()
	args := redis.XReadArgs{
		Streams: []string{r.streamName, cursor},
		Block:   0,
	}
	return r.redisClient.XRead(ctx, &args).Result()
}
