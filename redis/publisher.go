package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/nullstone-io/go-streaming/stream"
	"log"
	"time"
)

var _ stream.Publisher = &Publisher{}

type Publisher struct {
	redisClient *redis.Client
}

func NewPublisher(redisClient *redis.Client) *Publisher {
	return &Publisher{
		redisClient: redisClient,
	}
}

func (p *Publisher) PublishLogs(strm string, id *int64, phase string, logs string) {
	m := stream.Message{
		Context: phase,
		Content: logs,
	}
	p.publish(strm, id, m)
}

func (p *Publisher) PublishObject(strm string, id *int64, event stream.EventType, object interface{}) {
	data, err := json.Marshal(object)
	if err != nil {
		log.Printf("error marshalling message content: %v", err)
	}
	m := stream.Message{
		Context: string(event),
		Content: string(data),
	}
	p.publish(strm, id, m)
}

func (p *Publisher) PublishEot(strm string) {
	m := stream.Message{
		Context: "eot",
		Content: stream.EndOfTransmission,
	}
	p.publish(strm, nil, m)
}

func (p *Publisher) publish(strm string, id *int64, message stream.Message) {
	args := redis.XAddArgs{
		ID:     fmt.Sprintf("%d-*", id),
		Stream: strm,
		Values: message.ToMap(),
	}
	ctx := context.Background()

	p.redisClient.XAdd(ctx, &args)
	// with every new log that we publish, reset the expiry on the redis stream
	// the stream will automatically be removed in redis an hour after the final activity
	p.redisClient.Expire(ctx, strm, time.Hour)
}
