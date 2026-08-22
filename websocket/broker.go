package websocket

import (
	"context"
	"fmt"
	"github.com/BSick7/go-api/json"
	"github.com/gorilla/websocket"
	"github.com/nullstone-io/go-streaming/stream"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	PingMessage = "ping"
	PongMessage = "pong"
)

type Broker struct {
	conn      *websocket.Conn
	messages  <-chan stream.Message
	errors    <-chan error
	done      chan struct{}
	mu        sync.Mutex
	handshake *handshake
}

func StartBroker(w *json.ResponseWriter, r *json.Request, msgs <-chan stream.Message, errs <-chan error) (*Broker, error) {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	hs := handshakeFromContext(r.Context())

	conn, err := upgrader.Upgrade(w.ResponseWriter, r.Request, nil)
	if err != nil {
		err = fmt.Errorf("unable to upgrade to websocket connection: %s", err)
		if hs != nil {
			hs.span.RecordError(err)
			hs.span.SetStatus(codes.Error, err.Error())
			hs.end()
		}
		return nil, err
	}
	if hs != nil {
		hs.span.SetAttributes(semconv.HTTPResponseStatusCode(http.StatusSwitchingProtocols))
	}

	broker := &Broker{
		conn:      conn,
		messages:  msgs,
		errors:    errs,
		done:      make(chan struct{}),
		handshake: hs,
	}

	go broker.writeLoop()
	go broker.readLoop()

	// The request's context is cancelled when its handler returns, long before the client disconnects.
	metricCtx := context.WithoutCancel(r.Context())
	metricAttrs := routeAttr(hs)
	activeConnections.Add(metricCtx, 1, metricAttrs)
	startedAt := time.Now()

	// This waits on done directly rather than calling WaitForClose, which ends the handshake span -
	// doing that here would close the span out the moment StartBroker returned. It also runs whether
	// or not the handler ever waits on the connection, so the counter stays balanced for handlers
	// that return without waiting.
	go func() {
		<-broker.done
		activeConnections.Add(metricCtx, -1, metricAttrs)
		connectionDuration.Record(metricCtx, time.Since(startedAt).Seconds(), metricAttrs)
	}()

	return broker, nil
}

func (b *Broker) writeLoop() {
	defer b.conn.Close()

	for {
		select {
		case message, ok := <-b.messages:
			if !ok {
				return
			}
			hasEot := strings.HasSuffix(message.Content, stream.EndOfTransmission)
			message.Content = strings.TrimSuffix(message.Content, stream.EndOfTransmission)
			if len(message.Content) > 0 {
				b.writeJsonMessage(message)
			}
			if hasEot {
				closeData := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "end of transmission")
				b.writeRawMessage(websocket.CloseMessage, closeData)
			}
		case err, ok := <-b.errors:
			if !ok {
				return
			}
			// In the websocket protocol (RFC 6455), a close frame payload cannot exceed 125 bytes
			// Instead of truncating a long error message, we're going to send the error message first, then send a close
			b.writeJsonMessage(stream.Message{
				Context: "error",
				Content: err.Error(),
			})
			b.writeRawMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, ""))

		case <-b.done:
			return
		}
	}
}

func (b *Broker) readLoop() {
	defer close(b.done)
	// keep this request alive intentionally until the client disconnects
	// a clean disconnect from the client will cause ReadMessage to return an error
	// we will simply log the "error" and end the function (causing everything to clean up)
	for {
		msgType, msg, err := b.conn.ReadMessage()
		if msgType == websocket.CloseMessage || err != nil {
			return
		}
		// respond to ping messages so the client knows we're still alive
		// the @vueuse/useWebSocket library forces us to send the pong as a text message
		// normally we would send a message with type PongMessage
		// even though this is a pong message, the spec says to respond with the same contents as the ping
		if msgType == websocket.PingMessage || string(msg) == PingMessage {
			b.writeRawMessage(websocket.TextMessage, msg)
		}
	}
}

func (b *Broker) writeRawMessage(messageType int, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn.WriteMessage(messageType, data)
}

func (b *Broker) writeJsonMessage(msg stream.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn.WriteJSON(msg)
}

// WaitForClose blocks until the client disconnects.
//
// Reaching it means the handler is done setting the connection up - established, hydrated, listeners
// started - and is now only holding it open, so the handshake span ends here rather than running for
// the life of the connection.
func (b *Broker) WaitForClose() {
	if b.handshake != nil {
		b.handshake.end()
	}
	<-b.done
}
