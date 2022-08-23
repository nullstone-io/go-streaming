package websocket

const (
	endOfTransmission = "\x04"
)

type Message struct {
	Source  string `json:"source"`
	Context string `json:"context"`
	Content string `json:"content"`
}

func NewEotMessage(source string) *Message {
	return &Message{
		Source:  source,
		Context: "eot",
		Content: endOfTransmission,
	}
}
