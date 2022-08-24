package redis

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	fileListenerLogger = log.New(os.Stderr, "[file-listener] ", 0)
)

type FileListener struct {
	filename  string
	publisher TextPublisher
	phase     string
	stream    string
	finish    chan bool // finish is signaled when the file listener should close
	done      chan bool // done is signaled when file listener has completed cleanup
}

func NewFileListener(filename string, publisher TextPublisher, stream string, phase string) *FileListener {
	return &FileListener{
		filename:  filename,
		publisher: publisher,
		phase:     phase,
		stream:    stream,
		finish:    make(chan bool),
		done:      make(chan bool),
	}
}

func (l *FileListener) Start() {
	go l.readLoop()
}

func (l *FileListener) Finish() {
	close(l.finish)
	<-l.done
}

func (l *FileListener) readLoop() {
	defer close(l.done)

	// file typically hasn't been created yet
	// go ahead and create it and be ready to begin reading from it
	file, err := os.OpenFile(l.filename, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		fileListenerLogger.Printf("unable to initialize file listener for file: %s", l.filename)
	}
	defer file.Close()

	// make sure to finish reading anything remaining in the file before we are done
	defer l.readToEnd(file)

	for {
		l.readToEnd(file)
		select {
		case <-time.After(100 * time.Millisecond):
		case <-l.finish:
			return
		}
	}
}

func (l *FileListener) readToEnd(file *os.File) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		l.publisher.Notify(LogMessage{
			Stream: l.stream,
			Phase:  l.phase,
			Logs:   fmt.Sprintf("%s\n", scanner.Text()),
		})
	}

	if err := scanner.Err(); err != nil {
		fileListenerLogger.Printf("error scanning file: %s", file.Name())
	}
}
