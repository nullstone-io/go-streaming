package websocket

import (
	"fmt"
	"github.com/BSick7/go-api/json"
	"github.com/gorilla/websocket"
	"github.com/nullstone-io/go-streaming/stream"
	"log"
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
	go func() {
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
				closeData := websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())
				b.conn.WriteMessage(websocket.CloseMessage, closeData)

			case <-b.done:
				return
			}
		}
	}()
}

func (b *Broker) readLoop() {
	defer close(b.done)
	// keep this request alive intentionally until the client disconnects
	// a clean disconnect from the client will cause ReadMessage to return an error
	// we will simply log the "error" and end the function (causing everything to clean up)
	for {
		msgType, msg, err := b.conn.ReadMessage()
		log.Printf("[WEBSOCKET MESSAGE] %d:%s", msgType, msg)
		if msgType == websocket.CloseMessage || err != nil {
			return
		}
		// respond to ping messages so the client knows we're still alive
		if msgType == websocket.PingMessage || string(msg) == PingMessage {
			b.conn.WriteMessage(websocket.PongMessage, []byte(PongMessage))
		}
	}
}

func (b *Broker) WaitForClose() {
	<-b.done
}
