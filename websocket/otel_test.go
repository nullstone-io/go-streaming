package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestAddAttrToOtelMiddleware(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name:    "websocket upgrade",
			headers: map[string]string{"Connection": "Upgrade", "Upgrade": "websocket"},
			want:    true,
		},
		{
			name: "plain request is marked false, not left unmarked",
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test")

			handler := AddAttrToOtelMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

			r := httptest.NewRequest(http.MethodGet, "/streams", nil)
			for name, value := range test.headers {
				r.Header.Set(name, value)
			}
			// Simulates otelmux: the server span is already active when this middleware runs.
			ctx, span := tracer.Start(r.Context(), "GET /streams", trace.WithSpanKind(trace.SpanKindServer))
			handler.ServeHTTP(httptest.NewRecorder(), r.WithContext(ctx))
			span.End()

			spans := recorder.Ended()
			require.Len(t, spans, 1)
			attrs := map[attribute.Key]attribute.Value{}
			for _, attr := range spans[0].Attributes() {
				attrs[attr.Key] = attr.Value
			}
			require.Contains(t, attrs, Attr, "the span carries no websocket attribute")
			assert.Equal(t, test.want, attrs[Attr].AsBool())
		})
	}

	t.Run("no active span is a no-op", func(t *testing.T) {
		handler := AddAttrToOtelMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		assert.NotPanics(t, func() {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/streams", nil))
		})
	})
}

func TestMetricAttrs(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/streams", nil)
	require.Equal(t, []attribute.KeyValue{Attr.Bool(false)}, MetricAttrs(r), "plain request must carry an explicit false")

	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	require.Equal(t, []attribute.KeyValue{Attr.Bool(true)}, MetricAttrs(r))
}
