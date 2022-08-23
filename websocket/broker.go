package websocket

import (
	"fmt"
	"github.com/BSick7/go-api/json"
	"github.com/gorilla/websocket"
	"net/http"
	"strings"
)

type Broker struct {
	conn     *websocket.Conn
	messages <-chan Message
	errors   <-chan error
	done     chan struct{}
}

func StartBroker(w *json.ResponseWriter, r *json.Request, msgs <-chan Message, errs <-chan error) (*Broker, error) {
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
			case message := <-b.messages:
				hasEot := strings.HasSuffix(message.Content, endOfTransmission)
				message.Content = strings.TrimSuffix(message.Content, endOfTransmission)
				if len(message.Content) > 0 {
					b.conn.WriteJSON(message)
				}
				if hasEot {
					closeData := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "end of transmission")
					b.conn.WriteMessage(websocket.CloseMessage, closeData)
				}
			case err := <-b.errors:
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
		msgType, _, err := b.conn.ReadMessage()
		if msgType == websocket.CloseMessage || err != nil {
			return
		}
	}
}

func (b *Broker) WaitForClose() {
	<-b.done
}
