package redis

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

type EventType string

const (
	EventTypeCreated EventType = "created"
	EventTypeUpdated EventType = "updated"
	EventTypeDeleted EventType = "deleted"
)

type ObjectMessage struct {
	Stream    string      // represents the type of data being published - e.g. 'ssickles:deploys' - where ssickles is the org name
	EventType EventType   // represents the event that generated the message, e.g. created, updated, deleted
	Data      interface{} // the data being published
}

type ObjectPublisher struct {
	redisClient *redis.Client
}

func NewObjectPublisher(redisClient *redis.Client) *ObjectPublisher {
	return &ObjectPublisher{
		redisClient: redisClient,
	}
}

func (l *ObjectPublisher) Publish(message ObjectMessage) {
	data, err := json.Marshal(message.Data)
	if err != nil {
		log.Printf("error marshalling message content: %v", err)
	}
	values := map[string]interface{}{"event": string(message.EventType), "data": data}
	args := redis.XAddArgs{
		Stream: message.Stream,
		Values: values,
	}
	ctx := context.Background()

	l.redisClient.XAdd(ctx, &args)
	// with every new log that we publish, reset the expiry on the redis stream
	// the stream will automatically be removed in redis an hour after the final activity
	l.redisClient.Expire(ctx, message.Stream, time.Hour)
}
