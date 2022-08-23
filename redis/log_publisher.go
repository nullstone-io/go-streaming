package redis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type logPubContextKey struct{}

func ContextWithLogPub(ctx context.Context, pub *LogPublisher) context.Context {
	return context.WithValue(ctx, logPubContextKey{}, pub)
}

func LogPubFromContext(ctx context.Context) *LogPublisher {
	if val, ok := ctx.Value(logPubContextKey{}).(*LogPublisher); ok {
		return val
	}
	return nil
}

type LogMessage struct {
	Stream string // represents where the message originated, e.g. - the deploy that generated the logs
	Phase  string // represents the action being taken that generated the message, e.g. - the phase of the deploy (e.g. init, checkout, build, etc.)
	Logs   string // the message text
}

type LogPublisher struct {
	redisClient *redis.Client
}

func NewLogPublisher(redisClient *redis.Client) *LogPublisher {
	return &LogPublisher{
		redisClient: redisClient,
	}
}

func (l *LogPublisher) Notify(message LogMessage) {
	values := map[string]interface{}{"phase": message.Phase, "line": message.Logs}
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
