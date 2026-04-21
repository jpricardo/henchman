package cache

import (
	"testing"
	"unsafe"
)

func TestEntryStructOverhead_OverheadSize(t *testing.T) {
	e := Entry{}
	us := unsafe.Sizeof(e)

	if us != entryStructOverhead {
		t.Errorf("Struct size %d doesn't match overhead constant %d", us, entryStructOverhead)
	}
}

func TestSizeBytes_OverheadSize(t *testing.T) {
	e := Entry{}
	s := e.SizeBytes()

	if s != int64(entryStructOverhead) {
		t.Errorf("Computed size %d doesn't match overhead constant %d", s, entryStructOverhead)
	}
}
