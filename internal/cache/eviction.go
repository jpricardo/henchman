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

// #region LFU
type lfuEntry struct {
	key       string
	sizeBytes int64
	freq      int64
}

type LFUEvictionPolicy struct {
	currentBytes int64
	currentKeys  int64
	evictions    int64
	minFreq      int64
	keyMap       map[string]*list.Element
	freqLists    map[int64]*list.List
}

func NewLFUEvictionPolicy() EvictionPolicy {
	return &LFUEvictionPolicy{
		keyMap:    make(map[string]*list.Element),
		freqLists: make(map[int64]*list.List),
	}
}

func (ep *LFUEvictionPolicy) Add(key string, sizeBytes int64) {
	if _, exists := ep.keyMap[key]; exists {
		ep.Remove(key)
	}

	l := ep.freqLists[1]
	if l == nil {
		l = list.New()
		ep.freqLists[1] = l
	}

	e := l.PushFront(lfuEntry{key: key, sizeBytes: sizeBytes, freq: 1})
	ep.keyMap[key] = e
	ep.currentBytes += sizeBytes
	ep.currentKeys++
	ep.minFreq = 1
}

func (ep *LFUEvictionPolicy) Touch(key string) {
	e := ep.keyMap[key]
	if e == nil {
		return
	}

	entry := e.Value.(lfuEntry)
	oldFreq := entry.freq
	newFreq := oldFreq + 1

	oldList := ep.freqLists[oldFreq]
	oldList.Remove(e)
	if oldList.Len() == 0 {
		delete(ep.freqLists, oldFreq)
		if ep.minFreq == oldFreq {
			ep.minFreq = newFreq
		}
	}

	newList := ep.freqLists[newFreq]
	if newList == nil {
		newList = list.New()
		ep.freqLists[newFreq] = newList
	}

	entry.freq = newFreq
	ep.keyMap[key] = newList.PushFront(entry)
}

func (ep *LFUEvictionPolicy) Remove(key string) {
	e := ep.keyMap[key]
	if e == nil {
		return
	}

	entry := e.Value.(lfuEntry)
	l := ep.freqLists[entry.freq]
	l.Remove(e)
	if l.Len() == 0 {
		delete(ep.freqLists, entry.freq)
	}

	delete(ep.keyMap, key)
	ep.currentBytes -= entry.sizeBytes
	ep.currentKeys--
}

func (ep *LFUEvictionPolicy) Evict() (key string, sizeBytes int64, ok bool) {
	if ep.currentKeys == 0 {
		return "", 0, false
	}

	l := ep.freqLists[ep.minFreq]
	for l == nil || l.Len() == 0 {
		ep.minFreq++
		l = ep.freqLists[ep.minFreq]
	}

	entry := l.Back().Value.(lfuEntry)
	ep.Remove(entry.key)
	ep.evictions++

	return entry.key, entry.sizeBytes, true
}

func (ep *LFUEvictionPolicy) Evictions() int64 {
	return ep.evictions
}

func (ep *LFUEvictionPolicy) Reset() {
	ep.currentBytes = 0
	ep.currentKeys = 0
	ep.minFreq = 0
	ep.keyMap = make(map[string]*list.Element)
	ep.freqLists = make(map[int64]*list.List)
}

func (ep *LFUEvictionPolicy) CurrentBytes() int64 {
	return ep.currentBytes
}

func (ep *LFUEvictionPolicy) CurrentKeys() int64 {
	return ep.currentKeys
}

// #endregion
