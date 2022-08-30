package stream

import "context"

type pubContextKey struct{}

func ContextWithPublisher(ctx context.Context, pub Publisher) context.Context {
	return context.WithValue(ctx, pubContextKey{}, pub)
}

func PublisherFromContext(ctx context.Context) Publisher {
	val, _ := ctx.Value(pubContextKey{}).(Publisher)
	return val
}

type Publisher interface {
	PublishLogs(stream string, id int, phase string, logs string)
	PublishObject(stream string, event EventType, object interface{})
	PublishEot(stream string)
}
