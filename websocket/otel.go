package websocket

import (
	"net/http"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Attr marks a request as a websocket upgrade. A websocket handler blocks until the client
// disconnects, so its span and its http.server.request.duration sample measure the connection
// rather than the request; marking them lets a latency view filter on http.websocket = false.
const Attr = attribute.Key("http.websocket")

// AddAttrToOtelMiddleware records Attr on the active server span. It must run after the middleware
// that starts that span (otelmux).
func AddAttrToOtelMiddleware() func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace.SpanFromContext(r.Context()).SetAttributes(Attr.Bool(websocket.IsWebSocketUpgrade(r)))
			handler.ServeHTTP(w, r)
		})
	}
}

// MetricAttrs records the same attribute on http metrics, which span attributes do not reach; pass
// it to otelmux's or otelhttp's WithMetricAttributesFn. An explicit false, rather than a key only
// present on websockets, keeps the filter simple.
func MetricAttrs(r *http.Request) []attribute.KeyValue {
	return []attribute.KeyValue{Attr.Bool(websocket.IsWebSocketUpgrade(r))}
}
