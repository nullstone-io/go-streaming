package websocket

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// A websocket connection stays open for as long as the client wants it, which makes it a bad fit for
// a span: a span is not exported until it ends, so a connection-lifetime span reaches the backend
// hours late (or never, if the process restarts first) and reports the connection's lifetime as
// request latency.
//
// Instead, Middleware traces the handshake as a short server span and StartBroker reports the
// connection itself as metrics. Servers that wrap requests in their own http instrumentation should
// exclude upgrade requests from it - with otelmux, that is otelmux.WithFilter(func(r *http.Request)
// bool { return !websocket.IsUpgrade(r) }).

const scopeName = "github.com/nullstone-io/go-streaming/websocket"

var (
	tracer = otel.Tracer(scopeName)

	activeConnections  metric.Int64UpDownCounter
	connectionDuration metric.Float64Histogram
)

func init() {
	// The global providers are usually installed after this runs; otel's globals delegate to them
	// once they exist, so instruments created here still record.
	meter := otel.Meter(scopeName)
	var err error
	if activeConnections, err = meter.Int64UpDownCounter("nullstone.websocket.connections.active",
		metric.WithDescription("Number of websocket connections currently open"),
		metric.WithUnit("{connection}"),
	); err != nil {
		log.Printf("unable to create websocket connections metric: %s\n", err)
	}
	if connectionDuration, err = meter.Float64Histogram("nullstone.websocket.connection.duration",
		metric.WithDescription("How long a websocket connection stayed open"),
		metric.WithUnit("s"),
	); err != nil {
		log.Printf("unable to create websocket connection duration metric: %s\n", err)
	}
}

// RouteNamer reports the route template that a request matched, e.g. "/orgs/{orgName}/runs".
//
// This package stays router-agnostic; callers snap in their own router. With gorilla/mux:
//
//	func websocketRoute(r *http.Request) string {
//		route := mux.CurrentRoute(r)
//		if route == nil {
//			return ""
//		}
//		tpl, err := route.GetPathTemplate()
//		if err != nil {
//			return ""
//		}
//		return tpl
//	}
//
// It must return a bounded set of values. Returning something per-request, such as r.URL.Path, would
// make the connection metrics unbounded in cardinality. Returning "" is fine and means "unnamed".
type RouteNamer func(*http.Request) string

// IsUpgrade reports whether the client asked to upgrade this request to a websocket.
//
// This deliberately matches what the Upgrader itself requires: both headers present, values compared
// case-insensitively (RFC 6455 does not constrain their case), and Connection read as a
// comma-separated token list. A caller filtering its http instrumentation with this needs it to
// agree with StartBroker, or a real websocket request slips through and gets a connection-lifetime
// span anyway.
func IsUpgrade(r *http.Request) bool {
	return headerContainsToken(r.Header, "Connection", "upgrade") &&
		headerContainsToken(r.Header, "Upgrade", "websocket")
}

// headerContainsToken reports whether a comma-separated header contains a token, across every line
// of that header. Both headers this is used for are defined as token lists, and the Upgrader reads
// them the same way - "Connection: keep-alive, Upgrade" is an upgrade request.
func headerContainsToken(header http.Header, name, value string) bool {
	for _, line := range header.Values(name) {
		for _, token := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(token), value) {
				return true
			}
		}
	}
	return false
}

// Middleware traces the handshake of websocket upgrade requests. Requests that are not upgrades pass
// through untouched.
//
// The span it starts is in the request context, so middleware that runs after it - panic recovery,
// error capture, whatever attributes the request to a user - records onto it as usual. The span ends
// when the handler stops setting the connection up and starts holding it open, which the handler
// signals by calling Broker.WaitForClose.
//
// routeName may be nil, in which case spans and metrics are reported without a route.
//
// StartBroker records connection metrics whether or not this is installed, but only reports a
// handshake span when it is.
func Middleware(routeName RouteNamer) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !IsUpgrade(r) {
				handler.ServeHTTP(w, r)
				return
			}

			// Callers filter upgrade requests out of their http instrumentation, which is also what
			// would normally extract the incoming trace context. Without this the connection would
			// start its own trace instead of continuing the caller's.
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			var route string
			if routeName != nil {
				route = routeName(r)
			}
			attrs := []attribute.KeyValue{
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
			}
			name := r.Method + " websocket"
			if route != "" {
				attrs = append(attrs, semconv.HTTPRoute(route))
				name = r.Method + " " + route
			}

			ctx, span := tracer.Start(ctx, name,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(attrs...),
			)
			hs := &handshake{span: span, route: route}
			// Safety net for a handler that returns without reaching WaitForClose - an early error
			// return, say. Without this the span would stay open for the life of the process.
			defer hs.end()

			handler.ServeHTTP(w, r.WithContext(context.WithValue(ctx, handshakeKey{}, hs)))
		})
	}
}

type handshakeKey struct{}

type handshake struct {
	span    trace.Span
	route   string
	endOnce sync.Once
}

func (h *handshake) end() {
	h.endOnce.Do(func() { h.span.End() })
}

func handshakeFromContext(ctx context.Context) *handshake {
	hs, _ := ctx.Value(handshakeKey{}).(*handshake)
	return hs
}

// routeAttr labels connection metrics. Unnamed routes are bucketed together rather than falling back
// to the url path, which would make the metrics unbounded.
func routeAttr(hs *handshake) metric.MeasurementOption {
	route := "unknown"
	if hs != nil && hs.route != "" {
		route = hs.route
	}
	return metric.WithAttributes(semconv.HTTPRoute(route))
}
