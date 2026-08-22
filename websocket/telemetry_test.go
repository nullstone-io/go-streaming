package websocket

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BSick7/go-api/json"
	"github.com/gorilla/websocket"
	"github.com/nullstone-io/go-streaming/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The route template these tests pretend a router matched. This package is router-agnostic, so the
// tests supply their own RouteNamer rather than importing gorilla/mux - which also keeps gorilla/mux
// out of this module's direct requires. The real mux adapter is exercised in nullfire and enigma.
const testRoute = "/orgs/{orgName}/runs"

var (
	spanRecorder *tracetest.SpanRecorder
	metricReader *sdkmetric.ManualReader
)

// TestMain installs the sdk providers once. otel's globals only delegate to the first provider they
// are given, and the tracer and instruments in this package are created at init, so this cannot be
// done per-test.
func TestMain(m *testing.M) {
	spanRecorder = tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))

	metricReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))

	// Apps install a propagator via telemetry.Start; without one, Extract is a no-op.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	os.Exit(m.Run())
}

func TestIsUpgrade(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string][]string
		want    bool
	}{
		{
			name:    "canonical upgrade",
			headers: map[string][]string{"Connection": {"Upgrade"}, "Upgrade": {"websocket"}},
			want:    true,
		},
		{
			name:    "rfc 6455 does not constrain the case of these values",
			headers: map[string][]string{"Connection": {"upgrade"}, "Upgrade": {"WebSocket"}},
			want:    true,
		},
		{
			name:    "connection carries a token list",
			headers: map[string][]string{"Connection": {"keep-alive, Upgrade"}, "Upgrade": {"websocket"}},
			want:    true,
		},
		{
			name:    "upgrade token is not last in the list",
			headers: map[string][]string{"Connection": {"Upgrade, keep-alive"}, "Upgrade": {"websocket"}},
			want:    true,
		},
		{
			name:    "upgrade carries a token list - the upgrader accepts this, so this must too",
			headers: map[string][]string{"Connection": {"Upgrade"}, "Upgrade": {"websocket, h2c"}},
			want:    true,
		},
		{
			name:    "tokens split across repeated header lines",
			headers: map[string][]string{"Connection": {"keep-alive", "Upgrade"}, "Upgrade": {"websocket"}},
			want:    true,
		},
		{
			name:    "generous whitespace",
			headers: map[string][]string{"Connection": {"  keep-alive ,   Upgrade  "}, "Upgrade": {" websocket "}},
			want:    true,
		},
		{
			name:    "plain http request",
			headers: map[string][]string{},
			want:    false,
		},
		{
			name:    "upgrade to a different protocol",
			headers: map[string][]string{"Connection": {"Upgrade"}, "Upgrade": {"h2c"}},
			want:    false,
		},
		{
			name:    "upgrade header without a connection token",
			headers: map[string][]string{"Upgrade": {"websocket"}},
			want:    false,
		},
		{
			name:    "connection token without an upgrade header",
			headers: map[string][]string{"Connection": {"Upgrade"}},
			want:    false,
		},
		{
			name:    "tokens must match whole, not by prefix",
			headers: map[string][]string{"Connection": {"Upgraded"}, "Upgrade": {"websockets"}},
			want:    false,
		},
		{
			name:    "empty header values",
			headers: map[string][]string{"Connection": {""}, "Upgrade": {""}},
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/orgs/nullstone/runs", nil)
			for name, values := range test.headers {
				for _, value := range values {
					r.Header.Add(name, value)
				}
			}
			assert.Equal(t, test.want, IsUpgrade(r))
		})
	}
}

// TestHandshakeSpan covers the reason this instrumentation exists: the span has to end at the
// handshake. A span is not exported until it ends, so a span that lasted as long as the connection
// would reach the backend hours late and would report the connection's lifetime as request latency.
func TestHandshakeSpan(t *testing.T) {
	// Modelled on nullfire's WebsocketEndpoint: upgrade, hydrate, start listening, then hold open.
	t.Run("ends while the connection is still open", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		msgs := make(chan stream.Message, 1)
		hydrated := make(chan struct{})
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, msgs, make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			msgs <- stream.Message{Context: "hydrate", Content: `{"runs":[]}`}
			close(hydrated)
			broker.WaitForClose()
		})

		conn := fixture.dial(t)
		defer conn.Close()
		<-hydrated

		// The client has not disconnected, so an unended span would still be sitting in the sdk.
		span := fixture.awaitSpan(t)
		assert.Equal(t, "GET "+testRoute, span.Name())
		assert.Equal(t, trace.SpanKindServer, span.SpanKind())
		assert.Equal(t, codes.Unset, span.Status().Code)

		attrs := attrsOf(span)
		assert.Equal(t, testRoute, attrs["http.route"].AsString())
		assert.Equal(t, "GET", attrs["http.request.method"].AsString())
		assert.Equal(t, "/orgs/nullstone/runs", attrs["url.path"].AsString())
		assert.Equal(t, int64(http.StatusSwitchingProtocols), attrs["http.response.status_code"].AsInt64())
	})

	// Modelled on enigma's WebsocketEndpoint, which registers `defer broker.WaitForClose()` and so
	// ends the span when the handler body finishes rather than at an explicit trailing call.
	t.Run("ends when a deferred WaitForClose runs", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		bodyDone := make(chan struct{})
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			defer broker.WaitForClose()
			close(bodyDone)
		})

		conn := fixture.dial(t)
		defer conn.Close()
		<-bodyDone

		fixture.awaitSpan(t)
	})

	// Modelled on nullfire's workspaces.go, which returns on a store error without ever waiting.
	t.Run("ends when a handler returns without waiting", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		returned := make(chan struct{})
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			_, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			assert.NoError(t, err)
			close(returned)
		})

		conn := fixture.dial(t)
		defer conn.Close()
		<-returned

		fixture.awaitSpan(t)
	})

	// The watcher goroutine that decrements the connection gauge waits on done directly. If it ever
	// went through WaitForClose it would end the span the instant StartBroker returned.
	t.Run("does not end before the handler waits", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		started := make(chan struct{})
		release := make(chan struct{})
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			close(started)
			<-release
			broker.WaitForClose()
		})

		conn := fixture.dial(t)
		defer conn.Close()
		<-started

		time.Sleep(50 * time.Millisecond)
		assert.Empty(t, fixture.spans(), "span ended before the handler finished setting up")

		close(release)
		fixture.awaitSpan(t)
	})

	t.Run("records a failed upgrade", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			_, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			assert.Error(t, err)
		})

		// Looks like an upgrade to IsUpgrade, but the Upgrader rejects it for the missing key and
		// version headers.
		req, err := http.NewRequest(http.MethodGet, fixture.server.URL+"/orgs/nullstone/runs", nil)
		require.NoError(t, err)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()

		span := fixture.awaitSpan(t)
		assert.Equal(t, codes.Error, span.Status().Code)
		require.Len(t, span.Events(), 1)
		assert.Equal(t, "exception", span.Events()[0].Name)
	})

	t.Run("continues the caller's trace", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			broker.WaitForClose()
		})

		// A traceparent as api-gateway would forward it.
		header := http.Header{}
		header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		conn := fixture.dial(t, header)
		defer conn.Close()

		span := fixture.awaitSpan(t)
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", span.SpanContext().TraceID().String())
		assert.Equal(t, "00f067aa0ba902b7", span.Parent().SpanID().String())
	})

	t.Run("names the span without a route when none is available", func(t *testing.T) {
		for _, namer := range []struct {
			name  string
			apply fixtureOption
		}{
			{name: "nil RouteNamer", apply: WithNilRouteNamer()},
			{name: "RouteNamer returns empty", apply: WithRoute("")},
		} {
			t.Run(namer.name, func(t *testing.T) {
				fixture := newFixture(t, namer.apply)

				fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
					broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
					if !assert.NoError(t, err) {
						return
					}
					broker.WaitForClose()
				})

				conn := fixture.dial(t)
				defer conn.Close()

				span := fixture.awaitSpan(t)
				assert.Equal(t, "GET websocket", span.Name())
				assert.NotContains(t, attrsOf(span), attribute.Key("http.route"))
			})
		}
	})

	t.Run("leaves non-upgrade requests to the caller's instrumentation", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			w.Send(map[string]string{"hello": "world"})
		})

		res, err := http.Get(fixture.server.URL + "/orgs/nullstone/runs")
		require.NoError(t, err)
		defer res.Body.Close()

		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Empty(t, fixture.spans())
	})

	t.Run("survives repeated and concurrent WaitForClose", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					broker.WaitForClose()
				}()
			}
			broker.WaitForClose()
			wg.Wait()
		})

		conn := fixture.dial(t)
		fixture.awaitSpan(t)

		require.NoError(t, conn.Close())
		fixture.awaitClosed(t)
		assert.Len(t, fixture.spans(), 1, "the span must be ended exactly once")
	})
}

// TestConnectionMetrics covers what replaces the connection-lifetime span. If these do not balance,
// a leak is invisible - the whole point is that nothing else reports an open connection.
func TestConnectionMetrics(t *testing.T) {
	t.Run("counts a connection for as long as it is open", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		before := readHistogramCount(t, testRoute)
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			broker.WaitForClose()
		})

		conn := fixture.dial(t)
		require.Eventually(t, func() bool { return readActiveConnections(t, testRoute) == 1 },
			5*time.Second, 10*time.Millisecond)

		require.NoError(t, conn.Close())
		fixture.awaitClosed(t)
		assert.Equal(t, int64(0), readActiveConnections(t, testRoute))
		assert.Equal(t, before+1, readHistogramCount(t, testRoute), "connection duration was not recorded")
	})

	// nullfire's run_logs.go sends EOT for a run that has already finished, so the server closes the
	// connection rather than the client.
	t.Run("counts down when the server ends the transmission", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		msgs := make(chan stream.Message, 1)
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, msgs, make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			msgs <- stream.EotMessage()
			broker.WaitForClose()
		})

		conn := fixture.dial(t)
		defer conn.Close()

		// Read until the server's close frame arrives.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}

		fixture.awaitClosed(t)
		assert.Equal(t, int64(0), readActiveConnections(t, testRoute))
	})

	// nullfire's workspaces.go returns on a store error without waiting; the counter still has to
	// come back down, or every such error would look like a permanently open connection.
	t.Run("counts down for a handler that never waits", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			_, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			assert.NoError(t, err)
		})

		conn := fixture.dial(t)
		require.NoError(t, conn.Close())

		fixture.awaitClosed(t)
		assert.Equal(t, int64(0), readActiveConnections(t, testRoute))
	})

	t.Run("counts concurrent connections", func(t *testing.T) {
		fixture := newFixture(t, WithRoute(testRoute))

		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			broker.WaitForClose()
		})

		const connections = 5
		conns := make([]*websocket.Conn, 0, connections)
		for i := 0; i < connections; i++ {
			conns = append(conns, fixture.dial(t))
		}
		require.Eventually(t, func() bool { return readActiveConnections(t, testRoute) == connections },
			5*time.Second, 10*time.Millisecond)

		for _, conn := range conns {
			require.NoError(t, conn.Close())
		}
		require.Eventually(t, func() bool { return readActiveConnections(t, testRoute) == 0 },
			5*time.Second, 10*time.Millisecond)
	})

	// A caller that upgrades without installing Middleware gets no handshake span, but must not
	// panic and must still be counted.
	t.Run("works without the middleware installed", func(t *testing.T) {
		fixture := newFixture(t, WithoutMiddleware())

		waited := make(chan struct{})
		fixture.handle(func(w *json.ResponseWriter, r *json.Request) {
			broker, err := StartBroker(w, r, make(chan stream.Message), make(chan error))
			if !assert.NoError(t, err) {
				return
			}
			close(waited)
			broker.WaitForClose()
		})

		conn := fixture.dial(t)
		<-waited
		require.Eventually(t, func() bool { return readActiveConnections(t, "unknown") == 1 },
			5*time.Second, 10*time.Millisecond)
		assert.Empty(t, fixture.spans())

		require.NoError(t, conn.Close())
		fixture.awaitClosed(t)
		assert.Equal(t, int64(0), readActiveConnections(t, "unknown"))
	})
}

type fixture struct {
	server      *httptest.Server
	handler     json.HandlerFunc
	handlerOnce sync.Once
	handlerSet  chan struct{}
}

type fixtureOption func(*fixtureConfig)

type fixtureConfig struct {
	routeName     RouteNamer
	useMiddleware bool
}

func WithRoute(route string) fixtureOption {
	return func(c *fixtureConfig) {
		c.routeName = func(*http.Request) string { return route }
	}
}

func WithNilRouteNamer() fixtureOption {
	return func(c *fixtureConfig) { c.routeName = nil }
}

func WithoutMiddleware() fixtureOption {
	return func(c *fixtureConfig) { c.useMiddleware = false }
}

// newFixture stands up a server for one test. Span and metric state is process-wide, so tests share
// it: the recorder is reset here, and every test is expected to leave the connection gauge at zero.
func newFixture(t *testing.T, opts ...fixtureOption) *fixture {
	t.Helper()

	cfg := fixtureConfig{useMiddleware: true}
	for _, opt := range opts {
		opt(&cfg)
	}

	spanRecorder.Reset()

	f := &fixture{handlerSet: make(chan struct{})}
	var handler http.Handler = json.Handler(func(w *json.ResponseWriter, r *json.Request) {
		<-f.handlerSet
		f.handler(w, r)
	})
	if cfg.useMiddleware {
		handler = Middleware(cfg.routeName)(handler)
	}

	mux := http.NewServeMux()
	mux.Handle("/orgs/nullstone/runs", handler)
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)

	return f
}

func (f *fixture) handle(handler json.HandlerFunc) {
	f.handler = handler
	f.handlerOnce.Do(func() { close(f.handlerSet) })
}

func (f *fixture) dial(t *testing.T, headers ...http.Header) *websocket.Conn {
	t.Helper()

	var header http.Header
	if len(headers) > 0 {
		header = headers[0]
	}
	url := "ws" + strings.TrimPrefix(f.server.URL, "http") + "/orgs/nullstone/runs"
	conn, _, err := websocket.DefaultDialer.Dial(url, header)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func (f *fixture) spans() []sdktrace.ReadOnlySpan {
	return spanRecorder.Ended()
}

func (f *fixture) awaitSpan(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()

	require.Eventually(t, func() bool { return len(f.spans()) >= 1 }, 5*time.Second, 10*time.Millisecond,
		"the handshake span never ended")
	return f.spans()[0]
}

// awaitClosed waits for the connection watcher to observe the disconnect and record its metrics.
func (f *fixture) awaitClosed(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool { return readActiveConnections(t, testRoute)+readActiveConnections(t, "unknown") == 0 },
		5*time.Second, 10*time.Millisecond, "a connection was never counted back down")
}

func attrsOf(span sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	attrs := map[attribute.Key]attribute.Value{}
	for _, attr := range span.Attributes() {
		attrs[attr.Key] = attr.Value
	}
	return attrs
}

func readActiveConnections(t *testing.T, route string) int64 {
	t.Helper()

	var value int64
	forEachDataPoint(t, "nullstone.websocket.connections.active", func(m metricdata.Aggregation) {
		sum, ok := m.(metricdata.Sum[int64])
		require.True(t, ok, "expected an int64 sum")
		for _, point := range sum.DataPoints {
			if routeOf(point.Attributes) == route {
				value = point.Value
			}
		}
	})
	return value
}

func readHistogramCount(t *testing.T, route string) uint64 {
	t.Helper()

	var count uint64
	forEachDataPoint(t, "nullstone.websocket.connection.duration", func(m metricdata.Aggregation) {
		hist, ok := m.(metricdata.Histogram[float64])
		require.True(t, ok, "expected a float64 histogram")
		for _, point := range hist.DataPoints {
			if routeOf(point.Attributes) == route {
				count = point.Count
			}
		}
	})
	return count
}

func forEachDataPoint(t *testing.T, name string, fn func(metricdata.Aggregation)) {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		if scope.Scope.Name != scopeName {
			continue
		}
		for _, m := range scope.Metrics {
			if m.Name == name {
				fn(m.Data)
			}
		}
	}
}

func routeOf(set attribute.Set) string {
	value, ok := set.Value("http.route")
	if !ok {
		return fmt.Sprintf("<missing http.route in %s>", set.Encoded(attribute.DefaultEncoder()))
	}
	return value.AsString()
}
