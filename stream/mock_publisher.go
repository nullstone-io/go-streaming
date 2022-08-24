package stream

import (
	"github.com/stretchr/testify/mock"
)

var _ Publisher = &MockPublisher{}

type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) PublishLogs(strm string, phase string, logs string) {
	m.MethodCalled("PublishLogs", strm, phase, logs)
}

func (m *MockPublisher) PublishObject(strm string, event EventType, object interface{}) {
	m.MethodCalled("PublishObject", strm, event, object)
}
