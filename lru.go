package gostore

import (
	"container/list"
	"time"
)

type lru struct {
	evictList *list.List
	items     map[string]*list.Element
	size      int
}

// entry is used to hold a value in the evictList
type entry struct {
	key    string
	expire time.Time
	value  any
}

func newLRU(size int) *lru {
	return &lru{
		evictList: list.New(),
		items:     make(map[string]*list.Element),
		size:      size,
	}
}

// Add adds a value to the cache.
func (l *lru) Add(key string, expire time.Time, value any) {
	// Check for existing item
	if ent, ok := l.items[key]; ok {
		l.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		return
	}

	// Add new item
	ent := &entry{key, expire, value}
	entry := l.evictList.PushFront(ent)
	l.items[key] = entry

	evict := l.evictList.Len() > l.size
	// Verify size not exceeded
	if evict {
		l.removeOldest()
	}
}

// Get looks up a key's value from the cache.
func (l *lru) Get(key string) (any, bool) {
	if ent, ok := l.items[key]; ok {
		l.evictList.MoveToFront(ent)
		if ent.Value.(*entry) == nil {
			return nil, false
		}
		if ent.Value.(*entry).expire.IsZero() || ent.Value.(*entry).expire.After(time.Now()) {
			return ent.Value.(*entry).value, true
		}
		l.removeElement(ent)
	}
	return nil, false
}

//Delete deletes a key from the cache.
func (l *lru) Delete(key string) {
	if ent, ok := l.items[key]; ok {
		l.removeElement(ent)
	}
}

// removeOldest removes the oldest item from the cache.
func (l *lru) removeOldest() {
	ent := l.evictList.Back()
	if ent != nil {
		l.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (l *lru) removeElement(e *list.Element) {
	l.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(l.items, kv.key)
}
