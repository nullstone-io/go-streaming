package stream

import (
	"github.com/stretchr/testify/mock"
)

var _ Publisher = &MockPublisher{}

type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) PublishLogs(strm string, id *int, phase string, logs string) {
	m.MethodCalled("PublishLogs", strm, id, phase, logs)
}

func (m *MockPublisher) PublishObject(strm string, id *int, event EventType, object interface{}) {
	m.MethodCalled("PublishObject", strm, id, event, object)
}

func (m *MockPublisher) PublishEot(strm string) {
	m.MethodCalled("PublishEot", strm)
}
