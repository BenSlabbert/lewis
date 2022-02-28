package writer

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"lewis/pkg/util"
	"testing"
)

func TestWriter_ReadFromBeginning(t *testing.T) {
	idFile, err := ioutil.TempFile("", "id_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(idFile)

	appendOnlyFile, err := ioutil.TempFile("", "append_only_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(appendOnlyFile)

	writer, err := NewWriter(appendOnlyFile.Name(), idFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer util.CloseQuietly(writer)

	for i := 1; i <= 2; i++ {
		writeId, err := writer.SyncWrite([]byte(fmt.Sprintf("hello%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if writeId != uint64(i) {
			t.Errorf("expected new id to be %d but was %d", i, writeId)
		}
	}

	messages, err := writer.ReadFromBeginning(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		m := <-messages
		if m.Err != nil {
			t.Fatal(err)
		}
		if m.Id != uint64(i) {
			t.Errorf("id to be %d but was %d", i, m.Id)
		}

		str := fmt.Sprintf("hello%d", i)
		if string(m.Body) != str {
			t.Errorf("body to be %s but was %s", str, string(m.Body))
		}
	}

	_, ok := <-messages
	if ok {
		t.Error("channel should be empty")
	}
}

func TestWriter_ReadMessage(t *testing.T) {
	idFile, err := ioutil.TempFile("", "id_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(idFile)

	appendOnlyFile, err := ioutil.TempFile("", "append_only_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(appendOnlyFile)

	writer, err := NewWriter(appendOnlyFile.Name(), idFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer util.CloseQuietly(writer)

	for i := 1; i <= 2; i++ {
		writeId, err := writer.SyncWrite([]byte(fmt.Sprintf("hello%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if writeId != uint64(i) {
			t.Errorf("expected new id to be %d but was %d", i, writeId)
		}
	}

	for i := 1; i <= 2; i++ {
		message, err := writer.ReadMessage(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		str := fmt.Sprintf("hello%d", i)
		if string(message) != str {
			t.Errorf("body to be %s but was %s", str, string(message))
		}
	}
}

func TestWriter_SubscribeToLatestMessages(t *testing.T) {
	idFile, err := ioutil.TempFile("", "id_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(idFile)

	appendOnlyFile, err := ioutil.TempFile("", "append_only_file")
	if err != nil {
		t.Fatal(err)
	}
	defer util.DeleteFileQuietly(appendOnlyFile)

	writer, err := NewWriter(appendOnlyFile.Name(), idFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer util.CloseQuietly(writer)

	u := uuid.New()
	messages := writer.SubscribeToLatestMessages(u)

	for i := 1; i <= 2; i++ {
		_, err = writer.SyncWrite([]byte(fmt.Sprintf("hello%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		m := <-messages

		if m.Id != uint64(i) {
			t.Errorf("expected id to be %d but was %d", i, m.Id)
		}
		str := fmt.Sprintf("hello%d", i)
		if string(m.Body) != str {
			t.Errorf("body to be %s but was %s", str, string(m.Body))
		}
	}

	writer.UnSubscribeToLatestMessages(u)
	// chan is now closed
	_, ok := <-messages
	if ok {
		t.Errorf("should be closed")
	}
}
