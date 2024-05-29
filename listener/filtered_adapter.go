package listener

import (
	"github.com/nullstone-io/go-streaming/redis"
	"github.com/nullstone-io/go-streaming/stream"
)

type FilterFunc func(message stream.Message) bool

var _ redis.Adapter = FilteredAdaptor{}

type FilteredAdaptor struct {
	BaseAdaptor redis.Adapter
	Filter      FilterFunc
}

func (f FilteredAdaptor) Send(message stream.Message) {
	if f.Filter(message) {
		f.BaseAdaptor.Send(message)
	}
}

func (f FilteredAdaptor) Flush() {
	f.BaseAdaptor.Flush()
}
