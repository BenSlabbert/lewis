package util

import (
	"encoding/binary"
	"io"
	"log"
	"os"
)

func CloseQuietly(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Println(err)
	}
}

func DeleteFileQuietly(f *os.File) {
	if err := os.Remove(f.Name()); err != nil {
		log.Println(err)
	}
}

func Uint64ToBytes(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func Uint64FromBytes(val []byte) uint64 {
	if len(val) == 8 {
		return binary.LittleEndian.Uint64(val)
	}
	return 0
}
