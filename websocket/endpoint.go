package websocket

import (
	"context"
	"fmt"
	"github.com/BSick7/go-api/json"
	"github.com/nullstone-io/go-streaming/stream"
)

func Endpoint(w *json.ResponseWriter, r *json.Request, fn func(ctx context.Context, msgs chan<- stream.Message, errs chan<- error)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgs := make(chan stream.Message)
	defer close(msgs)
	errs := make(chan error)
	defer close(errs)
	broker, err := StartBroker(w, r, msgs, errs)
	if err != nil {
		w.SendError(fmt.Errorf("unable to establish websocket connection"))
		return
	}

	fn(ctx, msgs, errs)

	broker.WaitForClose()
}
