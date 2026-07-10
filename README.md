# go-streaming

Building blocks for streaming real-time data from Nullstone services to browsers. A producer publishes messages (logs, object change events) onto a Redis Stream; an API endpoint tails that stream and relays each message over a websocket to the client.

```
 producer service                      consumer endpoint (e.g. nullfire, enigma)
┌──────────────────┐      ┌──────────────────────────────────────────────────┐
│ stream.Publisher │      │ redis.Listener → Adapter (chan) → websocket.Broker│
│  (redis XADD)    │─────▶│        (XREAD poll)                    (gorilla) │
└──────────────────┘      └──────────────────────────────────────────────────┘
        Redis Stream (expires 1h after last activity)          │
                                                               ▼
                                                            browser
```

## Packages

### `stream`

The shared vocabulary. `Message` is the unit that flows through every layer:

```go
type Message struct {
    Type    string         `json:"type"`
    Context string         `json:"context"`           // phase name or event type ("created", "updated", "hydrate", "eot", ...)
    Content string         `json:"content"`           // log text or a JSON-serialized object
    Details map[string]any `json:"details,omitempty"`
}
```

- `Publisher` is the producer-side interface (`PublishLogs`, `PublishObject`, `PublishEot`), with `ContextWithPublisher`/`PublisherFromContext` helpers for threading it through request contexts.
- `EndOfTransmission` (`\x04`) is the sentinel appended to `Content` to signal that a stream is complete; consumers use it to close the websocket cleanly.
- `MockPublisher` is a testify mock of `Publisher` for tests.

### `redis`

Redis Streams transport.

- `NewClient` builds a `go-redis` client from a URL, with optional pool sizing and OpenTelemetry instrumentation.
- `Publisher` implements `stream.Publisher` via `XADD`. Log messages use the log id as the entry ID so replays are ordered; every publish refreshes a 1-hour TTL on the stream, so streams evaporate an hour after the last activity.
- `Listener` tails a stream with `XREAD` (100ms block, 1s wait between polls so connections aren't held open) and forwards each entry to an `Adapter`. A cursor of `"-1"` means "start from now". It exits cleanly on context cancellation.

### `listener`

Adapters that bridge a `redis.Listener` to a consumer channel.

- `ChannelAdapter` — unbuffered pass-through; every message is delivered individually.
- `BufferedChannelAdapter` — coalesces log content and flushes when the buffer exceeds 1KB, when the message's `Context` (workflow phase) changes, or after a 1s tick. Use it for high-volume log streams so the websocket isn't flooded with tiny frames.

Both adapters are safe to `Close()` at any time, including while a producer is blocked mid-`Send` because the consumer stopped reading (e.g. the websocket client disconnected) — `Close()` releases the blocked sender instead of deadlocking, and `Send` after `Close` is a no-op. `BufferedChannelAdapter.Close()` makes a best-effort (1s-bounded) delivery of any remaining buffered content to a live consumer.

### `websocket`

`Broker` upgrades an HTTP request (gorilla/websocket) and pumps messages from an adapter's channel to the client:

- Messages are sent as JSON. A `Content` ending in `stream.EndOfTransmission` is trimmed and followed by a normal close frame.
- Errors received on the errors channel are sent as a `{"context": "error"}` message followed by a close frame (error text can exceed the 125-byte close-frame payload limit, so it travels as a message first).
- Replies to `ping` text messages with an echo, matching the heartbeat that `@vueuse/core`'s `useWebSocket` sends from the browser.
- `WaitForClose()` blocks until the client disconnects; writes are mutex-guarded so the write loop and pong replies don't interleave frames.

### `file`

`Listener` tails a local file (creating it if needed) and copies new lines to an `io.Writer` every 100ms until `Finish()` is called, reading any remainder before returning. Used to relay logs written to disk by external processes.

## Typical endpoint wiring

```go
msgs := listener.NewChannelAdapter()
defer msgs.Close()

broker, err := websocket.StartBroker(w, r, msgs.Channel(), errsCh)
if err != nil {
    return
}

// hydrate: send current state as the first message
msgs.Send(stream.Message{Context: "hydrate", Content: initialJSON})

// tail the redis stream until the client disconnects
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go redis.NewListener(redisClient, streamName, msgs).Listen(ctx, "-1")

broker.WaitForClose()
```

Order teardown so the context is cancelled before the adapter closes (deferred calls run in reverse): the stream listener stops producing, then the adapter releases anything still blocked.

## Development

```sh
make test   # go fmt + go test -v ./...
```

No Redis instance is required for the test suite. This module is consumed by `nullfire` and `enigma`; after merging changes here, bump `github.com/nullstone-io/go-streaming` in their `go.mod` and re-vendor.
