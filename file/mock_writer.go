package file

import (
	"github.com/nullstone-io/go-streaming/stream"
	"io"
)

var _ io.Writer = &MockWriter{}

type MockWriter struct {
	StreamName string
	Phase      string
	Publisher  *stream.MockPublisher
}

func (m *MockWriter) Write(p []byte) (n int, err error) {
	message := stream.Message{
		Context: m.Phase,
		Content: string(p),
	}
	m.Publisher.PublishLogs(m.StreamName, -1, message.Context, message.Content)
	return 0, nil
}
