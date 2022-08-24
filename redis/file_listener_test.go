package redis

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var _ TextPublisher = &TestNotifier{}

type TestNotifier struct {
	mock.Mock
}

func (t *TestNotifier) Notify(message LogMessage) {
	t.MethodCalled("Notify", message)
}

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
	expectedMessages := make([]LogMessage, len(lines))
	for i, line := range lines {
		expectedMessages[i] = LogMessage{
			Stream: source,
			Phase:  phase,
			Logs:   line,
		}
	}

	var notifier = new(TestNotifier)
	for _, m := range expectedMessages {
		notifier.On("Notify", m)
	}

	listener := NewFileListener(filepath.Join(dir, file.Name()), notifier, source, phase)
	listener.Start()
	defer func() {
		listener.Finish()
		notifier.AssertExpectations(t)
	}()

	for _, line := range lines {
		file.WriteString(line)
	}
}
