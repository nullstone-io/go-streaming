package file

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/nullstone-io/go-streaming/stream"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestFileListener(t *testing.T) {
	dir := "."
	file, err := ioutil.TempFile(dir, "log.out")
	require.NoError(t, err)
	defer os.Remove(filepath.Join(dir, file.Name()))
	defer file.Close()

	orgName := "nullstone"
	stackId := 5
	uid, err := uuid.NewUUID()
	require.NoError(t, err)
	source := fmt.Sprintf("%s:%d:%s", orgName, stackId, uid.String())
	phase := "plan"

	lines := []string{"Initializing...\n", "Fetching module\n", "Done\n"}
	expectedMessages := make([]stream.Message, len(lines))
	for i, line := range lines {
		expectedMessages[i] = stream.Message{
			Context: phase,
			Content: line,
		}
	}

	var publisher = new(stream.MockPublisher)
	for _, m := range expectedMessages {
		publisher.On("PublishLogs", source, m.Context, m.Content)
	}

	listener := NewFileListener(filepath.Join(dir, file.Name()), publisher, source, phase)
	listener.Start()
	defer func() {
		listener.Finish()
		publisher.AssertExpectations(t)
	}()

	for _, line := range lines {
		file.WriteString(line)
	}
}
