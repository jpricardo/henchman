package cache

import (
	"time"
	"unsafe"
)

type Entry struct {
	Key                 string
	Value               []byte
	ExpiresAt           time.Time
	InvalidateAfterRead bool
}

const entryStructOverhead = unsafe.Sizeof(Entry{})

func (e *Entry) SizeBytes() int64 {
	return int64(len(e.Key)) + int64(len(e.Value)) + int64(entryStructOverhead)
}
