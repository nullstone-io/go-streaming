package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"log"
)

func NewClient(ctx context.Context, redisUrl string, poolSize int) (*redis.Client, error) {
	if redisUrl == "" {
		return nil, fmt.Errorf("no redis url provided")
	}
	options, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	if poolSize > 0 {
		options.PoolSize = poolSize
	}

	client := redis.NewClient(options)
	if err := redisotel.InstrumentTracing(client); err != nil {
		log.Printf("unable to instrument redis with tracing: %s\n", err)
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		log.Printf("unable to instrument redis with metrics: %s\n", err)
	}

	pong, err := client.Ping(ctx).Result()
	if err != nil {
		log.Printf("ping request to the redis cluster failed: %s", err)
	} else {
		log.Printf("redis ping successful: %s", pong)
	}

	return client, nil
}
