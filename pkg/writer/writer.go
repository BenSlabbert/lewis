package writer

import (
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"lewis/pkg/util"
	"log"
	"os"
	"sync"
)

const KB = 1024

type Writer struct {
	currentOffset int64
	msgIndex      map[uint64]int64
	idxPath       string
	outPath       string

	out    *os.File
	outMtx sync.Mutex

	newMessageChan chan *Message

	subscriptions    map[uuid.UUID]chan *Message
	subscriptionsMtx sync.Mutex
}

type Message struct {
	Id   uint64
	Body []byte
}

type ReadFromBeginningMessage struct {
	Err  error
	Id   uint64
	Body []byte
}

// NewWriter creates a new writer instance with outPath being the destination path for the AOF
// and idxPath being the path for the file containing the latest message id.
func NewWriter(outPath, idxPath string) (*Writer, error) {
	outFile, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	currentOffset, err := outFile.Stat()
	if err != nil {
		return nil, err
	}

	w := &Writer{
		currentOffset:    currentOffset.Size(),
		msgIndex:         make(map[uint64]int64),
		idxPath:          idxPath,
		outPath:          outPath,
		out:              outFile,
		outMtx:           sync.Mutex{},
		newMessageChan:   make(chan *Message, 1),
		subscriptions:    make(map[uuid.UUID]chan *Message),
		subscriptionsMtx: sync.Mutex{},
	}

	id := w.getLatestId()
	log.Printf("last id was %d", id)

	go w.writeToSubscribers()

	return w, nil
}

func (w *Writer) Close() error {
	// stop all writes to this
	w.outMtx.Lock()
	return w.out.Close()
}

// getLatestId gets the latest id from file
func (w *Writer) getLatestId() uint64 {
	idxFile, err := os.OpenFile(w.idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer util.CloseQuietly(idxFile)

	// read this file to get the last id written
	fileInfo, err := idxFile.Stat()
	if err != nil {
		panic(err)
	}

	if fileInfo.Size() != 0 {
		// read the lastId
		bytes, err := ioutil.ReadAll(idxFile)
		if err != nil {
			panic(err)
		}

		return util.Uint64FromBytes(bytes)
	}

	return 0
}

// writeLatestId writes the latestId to file.
// Note: not thread safe
func (w *Writer) writeLatestId(id uint64) error {
	if err := os.Truncate(w.idxPath, 0); err != nil {
		return err
	}

	idxFile, err := os.OpenFile(w.idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer util.CloseQuietly(idxFile)

	b := util.Uint64ToBytes(id)
	_, err = idxFile.Write(b)
	return err
}

// SyncWrite allows only 1 goroutine at a time to write to the file
func (w *Writer) SyncWrite(bytes []byte) (uint64, error) {
	if len(bytes) > KB {
		return 0, fmt.Errorf("msg is to large %d, max is %d", len(bytes), KB)
	}

	w.outMtx.Lock()
	defer w.outMtx.Unlock()

	latestId := w.getLatestId() + 1
	err := w.writeLatestId(latestId)
	if err != nil {
		return 0, err
	}

	// write total length, id, bytes
	msgLength := uint64(len(bytes))
	all := make([]byte, 0, 8+8+msgLength)
	all = append(all, util.Uint64ToBytes(latestId)...)
	all = append(all, util.Uint64ToBytes(msgLength)...)
	all = append(all, bytes...)

	bytesWritten, err := w.out.Write(all)
	if err != nil {
		return 0, err
	}

	w.msgIndex[latestId] = w.currentOffset
	w.currentOffset += int64(bytesWritten)

	w.newMessageChan <- &Message{
		Id:   latestId,
		Body: bytes,
	}

	return latestId, err
}

// writeToSubscribers should be run in its own goroutine
func (w *Writer) writeToSubscribers() {
	for message := range w.newMessageChan {
		w.subscriptionsMtx.Lock()
		for _, c := range w.subscriptions {
			newC := c
			go func() {
				newC <- &Message{
					Id:   message.Id,
					Body: message.Body,
				}
			}()
		}
		w.subscriptionsMtx.Unlock()
	}
}

func (w *Writer) SubscribeToLatestMessages(u uuid.UUID) <-chan *Message {
	w.subscriptionsMtx.Lock()
	defer w.subscriptionsMtx.Unlock()

	msgChan := make(chan *Message, 1)

	w.subscriptions[u] = msgChan

	return msgChan
}

func (w *Writer) UnSubscribeToLatestMessages(u uuid.UUID) {
	w.subscriptionsMtx.Lock()
	defer w.subscriptionsMtx.Unlock()

	close(w.subscriptions[u])
	delete(w.subscriptions, u)
}

// ReadMessage reads a single message with the given id
func (w *Writer) ReadMessage(id uint64) ([]byte, error) {
	seekPoint, ok := w.msgIndex[id]
	if !ok {
		return nil, fmt.Errorf("no index for id %d", id)
	}

	file, err := os.Open(w.outPath)
	if err != nil {
		return nil, err
	}
	defer util.CloseQuietly(file)

	seek, err := file.Seek(seekPoint, io.SeekStart)
	if err != nil {
		return nil, err
	}

	if seek != seekPoint {
		return nil, fmt.Errorf("unable to seek to correct point")
	}

	readId, message, err := readMessage(file)

	if err != nil {
		return nil, err
	}

	if err == io.EOF {
		// eof before we could read the message
		return nil, fmt.Errorf("unable to read message, io.EOF before message could be read")
	}

	if id != readId {
		return nil, fmt.Errorf("incorrect messageId read %d for id %d", readId, id)
	}

	return message, nil
}

// readMessage reads a single message from the file.
// This function returns error io.EOF whe it has reached the end of the file.
func readMessage(file *os.File) (uint64, []byte, error) {
	// read first message id
	idBytes := make([]byte, 8)
	read, err := file.Read(idBytes)
	if read == 0 && err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
	}
	idUint64 := util.Uint64FromBytes(idBytes)

	// read message length
	msgLengthBytes := make([]byte, 8)
	read, err = file.Read(msgLengthBytes)
	if read == 0 && err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
	}
	msgLengthUint64 := util.Uint64FromBytes(msgLengthBytes)

	// read message body
	msgBytes := make([]byte, msgLengthUint64, msgLengthUint64)
	read, err = file.Read(msgBytes)
	if read == 0 && err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
	}

	return idUint64, msgBytes, nil
}

// ReadFromBeginning reads the entire file from the first message until the end of the file.
// When the file is read completely the chan will be closed.
// At this point there are no more message to read. Any errors while reading will appear
// in the same manner, if an error is encountered reading will stop at that point.
//
// Note: this method does not 'tail' the AOF
func (w *Writer) ReadFromBeginning() (<-chan *ReadFromBeginningMessage, error) {
	file, err := os.Open(w.outPath)
	if err != nil {
		return nil, err
	}
	// make sure we are at the beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	messageChan := make(chan *ReadFromBeginningMessage, 1)

	go func() {
		defer close(messageChan)

		for {
			id, msg, err := readMessage(file)
			if err == io.EOF {
				return
			}

			if err != nil {
				messageChan <- &ReadFromBeginningMessage{Err: err}
				return
			}

			messageChan <- &ReadFromBeginningMessage{
				Id:   id,
				Body: msg,
			}
		}
	}()

	return messageChan, nil
}
