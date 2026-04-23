package cache

import "container/list"

type EvictionPolicy interface {
	Add(key string, sizeBytes int64)
	Touch(key string)
	Remove(key string)
	Evict() (key string, sizeBytes int64, ok bool)
	Reset()
	CurrentBytes() int64
	CurrentKeys() int64
	Evictions() int64
}

// #region LRU
type lruEntry struct {
	key       string
	sizeBytes int64
}

type LRUEvictionPolicy struct {
	currentBytes int64
	evictions    int64
	list         list.List
	keyMap       map[string]*list.Element
}

func NewLRUEvictionPolicy() EvictionPolicy {
	return &LRUEvictionPolicy{
		currentBytes: 0,
		list:         list.List{},
		keyMap:       make(map[string]*list.Element),
	}
}

func (ep *LRUEvictionPolicy) Add(key string, sizeBytes int64) {
	if _, exists := ep.keyMap[key]; exists {
		ep.Remove(key)
	}

	e := ep.list.PushFront(lruEntry{key: key, sizeBytes: sizeBytes})
	ep.keyMap[key] = e
	ep.currentBytes += sizeBytes
}

func (ep *LRUEvictionPolicy) Touch(key string) {
	e := ep.keyMap[key]

	if e != nil {
		ep.list.MoveToFront(e)
		return
	}
}

func (ep *LRUEvictionPolicy) Remove(key string) {
	e := ep.keyMap[key]

	if e != nil {
		entry := ep.list.Remove(e).(lruEntry)
		ep.currentBytes -= entry.sizeBytes
	}

	delete(ep.keyMap, key)
}

func (ep *LRUEvictionPolicy) Evict() (key string, sizeBytes int64, ok bool) {
	b := ep.list.Back()
	if b == nil {
		return "", 0, false
	}

	entry := b.Value.(lruEntry)
	ep.Remove(entry.key)
	ep.evictions++

	return entry.key, entry.sizeBytes, true
}

func (ep *LRUEvictionPolicy) Evictions() int64 {
	return ep.evictions
}

func (ep *LRUEvictionPolicy) Reset() {
	ep.currentBytes = 0
	ep.keyMap = make(map[string]*list.Element)
	ep.list = list.List{}
}

func (ep *LRUEvictionPolicy) CurrentBytes() int64 {
	return ep.currentBytes
}

func (ep *LRUEvictionPolicy) CurrentKeys() int64 {
	return int64(ep.list.Len())
}

// #endregion
