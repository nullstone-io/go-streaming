package mock

import (
	"github.com/nullstone-io/go-streaming/redis"
	"github.com/stretchr/testify/mock"
)

var _ redis.TextPublisher = &MockLogPublisher{}

type MockLogPublisher struct {
	mock.Mock
}

func (m *MockLogPublisher) Notify(message redis.LogMessage) {
	m.MethodCalled("Notify", message)
}
