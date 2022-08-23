package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
)

func NewRedisClient(redisUrl string) (*redis.Client, error) {
	if redisUrl == "" {
		return nil, fmt.Errorf("no redis url provided")
	}
	options, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}
	client := redis.NewClient(options)
	ctx := context.Background()
	pong, err := client.Ping(ctx).Result()
	if err != nil {
		log.Printf("ping request to the redis cluster failed: %s", err)
	} else {
		log.Printf("redis ping successful: %s", pong)
	}
	return client, nil
}
