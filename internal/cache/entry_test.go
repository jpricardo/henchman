package cache

import "testing"

func TestSizeBytes_OverheadSize(t *testing.T) {
	e := Entry{}
	s := e.SizeBytes()

	if s != int64(entryStructOverhead) {
		t.Errorf("Struct size %d doesn't match overhead constant %d", s, entryStructOverhead)
	}
}
