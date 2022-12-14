package file

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var (
	fileListenerLogger = log.New(os.Stderr, "[file-listener] ", 0)
)

type Listener struct {
	Filename string
	writer   io.Writer
	finish   chan bool // finish is signaled when the file listener should close
	done     chan bool // done is signaled when file listener has completed cleanup
}

func NewListener(filename string, writer io.Writer) *Listener {
	return &Listener{
		Filename: filename,
		writer:   writer,
		finish:   make(chan bool),
		done:     make(chan bool),
	}
}

func (l *Listener) Start() {
	go l.readLoop()
}

func (l *Listener) Finish() {
	close(l.finish)
	<-l.done
}

func (l *Listener) readLoop() {
	defer close(l.done)

	// file typically hasn't been created yet
	// go ahead and create it and be ready to begin reading from it
	file, err := os.OpenFile(l.Filename, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		fileListenerLogger.Printf("unable to initialize file listener for file: %s", l.Filename)
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

func (l *Listener) readToEnd(file *os.File) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		_, err := l.writer.Write([]byte(fmt.Sprintf("%s\n", scanner.Text())))
		if err != nil {
			fileListenerLogger.Printf("error writing logs: %s", err)
		}
	}

	if err := scanner.Err(); err != nil {
		fileListenerLogger.Printf("error scanning file: %s", file.Name())
	}
}
