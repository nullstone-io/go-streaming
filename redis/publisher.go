package redis

import (
	"context"
	"encoding/json"
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

func (p *Publisher) PublishLogs(strm string, phase string, logs string) {
	m := stream.Message{
		Context: phase,
		Content: logs,
	}
	p.publish(strm, m)
}

func (p *Publisher) PublishObject(strm string, event stream.EventType, object interface{}) {
	data, err := json.Marshal(object)
	if err != nil {
		log.Printf("error marshalling message content: %v", err)
	}
	m := stream.Message{
		Context: string(event),
		Content: string(data),
	}
	p.publish(strm, m)
}

func (p *Publisher) PublishEot(strm string) {
	m := stream.Message{
		Context: "eot",
		Content: stream.EndOfTransmission,
	}
	p.publish(strm, m)
}

func (p *Publisher) publish(strm string, message stream.Message) {
	args := redis.XAddArgs{
		Stream: strm,
		Values: message.ToMap(),
	}
	ctx := context.Background()

	p.redisClient.XAdd(ctx, &args)
	// with every new log that we publish, reset the expiry on the redis stream
	// the stream will automatically be removed in redis an hour after the final activity
	p.redisClient.Expire(ctx, strm, time.Hour)
}
