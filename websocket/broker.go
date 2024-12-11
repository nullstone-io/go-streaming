package websocket

import (
	"fmt"
	"github.com/BSick7/go-api/json"
	"github.com/gorilla/websocket"
	"github.com/nullstone-io/go-streaming/stream"
	"net/http"
	"strings"
)

const (
	PingMessage = "ping"
	PongMessage = "pong"
)

type Broker struct {
	conn     *websocket.Conn
	messages <-chan stream.Message
	errors   <-chan error
	done     chan struct{}
}

func StartBroker(w *json.ResponseWriter, r *json.Request, msgs <-chan stream.Message, errs <-chan error) (*Broker, error) {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(w.ResponseWriter, r.Request, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to upgrade to websocket connection: %s", err)
	}

	broker := &Broker{
		conn:     conn,
		messages: msgs,
		errors:   errs,
		done:     make(chan struct{}),
	}

	go broker.writeLoop()
	go broker.readLoop()

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
				b.conn.WriteJSON(message)
			}
			if hasEot {
				closeData := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "end of transmission")
				b.conn.WriteMessage(websocket.CloseMessage, closeData)
			}
		case err, ok := <-b.errors:
			if !ok {
				return
			}
			// In the websocket protocol (RFC 6455), a close frame payload cannot exceed 125 bytes
			// Instead of truncating a long error message, we're going to send the error message first, then send a close
			b.conn.WriteJSON(stream.Message{
				Context: "error",
				Content: err.Error(),
			})
			b.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, ""))

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
			b.conn.WriteMessage(websocket.TextMessage, msg)
		}
	}
}

func (b *Broker) WaitForClose() {
	<-b.done
}
