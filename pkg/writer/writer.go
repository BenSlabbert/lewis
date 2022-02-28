package writer

import (
	"context"
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

	getLatestIdMtx   sync.Mutex
	writeLatestIdMtx sync.Mutex
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
	var (
		err           error
		outFile       *os.File
		currentOffset os.FileInfo
		w             *Writer
	)

	outFile, err = os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	currentOffset, err = outFile.Stat()
	if err != nil {
		return nil, err
	}

	w = &Writer{
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

	log.Printf("last id was %d", w.getLatestId())

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
	var (
		err      error
		idxFile  *os.File
		fileInfo os.FileInfo
		bytes    []byte
	)

	w.getLatestIdMtx.Lock()
	defer w.getLatestIdMtx.Unlock()

	idxFile, err = os.OpenFile(w.idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer util.CloseQuietly(idxFile)

	// read this file to get the last id written
	fileInfo, err = idxFile.Stat()
	if err != nil {
		panic(err)
	}

	if fileInfo.Size() != 0 {
		// read the lastId
		bytes, err = ioutil.ReadAll(idxFile)
		if err != nil {
			panic(err)
		}

		return util.Uint64FromBytes(bytes)
	}

	return 0
}

// writeLatestId writes the latestId to file.
func (w *Writer) writeLatestId(id uint64) error {
	var (
		err          error
		idxFile      *os.File
		int64AsBytes []byte
	)

	w.writeLatestIdMtx.Lock()
	defer w.writeLatestIdMtx.Unlock()

	if err = os.Truncate(w.idxPath, 0); err != nil {
		return err
	}

	idxFile, err = os.OpenFile(w.idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer util.CloseQuietly(idxFile)

	int64AsBytes = util.Uint64ToBytes(id)
	_, err = idxFile.Write(int64AsBytes)
	return err
}

// SyncWrite allows only 1 goroutine at a time to write to the file
func (w *Writer) SyncWrite(bytes []byte) (uint64, error) {
	var (
		err          error
		latestId     uint64
		bytesWritten int
		msg          *Message
	)

	latestId = w.getLatestId() + 1
	err = w.writeLatestId(latestId)
	if err != nil {
		return 0, err
	}

	bytesWritten, err = w.syncWriteToFile(latestId, bytes)
	if err != nil {
		return 0, err
	}

	w.msgIndex[latestId] = w.currentOffset
	w.currentOffset += int64(bytesWritten)

	msg = &Message{
		Id:   latestId,
		Body: bytes,
	}

	log.Println("send message to newMessageChan")
	w.newMessageChan <- msg
	log.Println("send message to newMessageChan... done")

	return latestId, err
}

func (w *Writer) syncWriteToFile(latestId uint64, bytes []byte) (int, error) {
	var (
		msgLength uint64
		all       []byte
	)

	if len(bytes) > KB {
		return 0, fmt.Errorf("msg is to large %d, max is %d", len(bytes), KB)
	}

	w.outMtx.Lock()
	defer w.outMtx.Unlock()

	// write total length, id, bytes
	msgLength = uint64(len(bytes))
	all = make([]byte, 0, 8+8+msgLength)
	all = append(all, util.Uint64ToBytes(latestId)...)
	all = append(all, util.Uint64ToBytes(msgLength)...)
	all = append(all, bytes...)

	return w.out.Write(all)
}

// writeToSubscribers should be run in its own goroutine
func (w *Writer) writeToSubscribers() {
	for message := range w.newMessageChan {
		w.subscriptionsMtx.Lock()
		log.Println("sending message to subscribers")
		for _, c := range w.subscriptions {
			c <- &Message{
				Id:   message.Id,
				Body: message.Body,
			}
		}
		log.Println("sending message to subscribers...done")
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
	// drain the chan to make sure there is a listener
	// for any message while we wait for the lock
	chanToClose := w.subscriptions[u]

	go func() {
		log.Printf("draining chan for subscription %s", u.String())
		for m := range chanToClose {
			log.Printf("%s drained %d", u.String(), m.Id)
		}
		log.Printf("subscription %s chan closed", u.String())
	}()

	w.subscriptionsMtx.Lock()
	defer w.subscriptionsMtx.Unlock()

	log.Printf("unsubscribing %s", u.String())
	close(chanToClose)
	delete(w.subscriptions, u)
}

// ReadMessage reads a single message with the given id
func (w *Writer) ReadMessage(id uint64) ([]byte, error) {
	var (
		err       error
		seekPoint int64
		ok        bool
		file      *os.File
		seek      int64
		readId    uint64
		message   []byte
	)

	seekPoint, ok = w.msgIndex[id]
	if !ok {
		return nil, fmt.Errorf("no index for id %d", id)
	}

	file, err = os.Open(w.outPath)
	if err != nil {
		return nil, err
	}
	defer util.CloseQuietly(file)

	seek, err = file.Seek(seekPoint, io.SeekStart)
	if err != nil {
		return nil, err
	}

	if seek != seekPoint {
		return nil, fmt.Errorf("unable to seek to correct point")
	}

	readId, message, err = readMessage(file)

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
	var (
		err             error
		idBytes         []byte
		read            int
		idUint64        uint64
		msgLengthBytes  []byte
		msgLengthUint64 uint64
		msgBytes        []byte
	)

	// read first message id
	idBytes = make([]byte, 8)
	read, err = file.Read(idBytes)
	if read == 0 && err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
	}
	idUint64 = util.Uint64FromBytes(idBytes)

	// read message length
	msgLengthBytes = make([]byte, 8)
	read, err = file.Read(msgLengthBytes)
	if read == 0 && err == io.EOF {
		return 0, nil, io.EOF
	}
	if err != nil {
		return 0, nil, err
	}
	msgLengthUint64 = util.Uint64FromBytes(msgLengthBytes)

	// read message body
	msgBytes = make([]byte, msgLengthUint64, msgLengthUint64)
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
func (w *Writer) ReadFromBeginning(ctx context.Context) (<-chan *ReadFromBeginningMessage, error) {
	var (
		err         error
		file        *os.File
		messageChan chan *ReadFromBeginningMessage
		id          uint64
		msg         []byte
	)

	file, err = os.Open(w.outPath)
	if err != nil {
		return nil, err
	}
	// make sure we are at the beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	messageChan = make(chan *ReadFromBeginningMessage, 1)

	go func() {
		defer close(messageChan)

		for {
			// stop reading on ctx err
			if ctx.Err() != nil {
				log.Println("context closed, exiting aof reader")
				return
			}

			id, msg, err = readMessage(file)
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
