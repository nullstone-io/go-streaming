package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/nullstone-io/go-streaming/stream"
	"github.com/redis/go-redis/v9"
	"time"
)

type Adapter interface {
	Send(message stream.Message)
	Flush()
	Close()
}

const waitDuration = 1 * time.Second

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
		cursor = fmt.Sprintf("%d", time.Now().UnixMilli())
	}

	for {
		args := redis.XReadArgs{
			Streams: []string{r.streamName, cursor},
			Block:   100 * time.Millisecond,
		}
		groups, err := r.redisClient.XRead(ctx, &args).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			} else if !errors.Is(err, redis.Nil) {
				// when no results are found, this lib returns a redis.Nil error
				// not exactly sure why but we want to continue listening
				// only return an error here if it's not a redis.Nil error
				return err
			}
		} else {
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
			// if we got results back, we don't want to wait for the timeout below
			// instead, loop back and query redis again immediately
			continue
		}
		select {
		// wait for a set period between each query to redis to not overload it
		// we do this instead of making the XRead call block so we don't hold open a connection to redis
		case <-time.After(waitDuration):
		case <-ctx.Done():
			return nil
		}
	}
}
