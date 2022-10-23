package gostore

import "testing"

func TestLRU(t *testing.T) {
	lru := newLRU(2)
	lru.Add("key1", "value1")
	lru.Add("key2", "value2")
	lru.Add("key3", "value3")
	lru.Add("key3", "value3")
	if v, ok := lru.Get("key1"); ok {
		t.Error("expected key1 to be evicted")
	} else if v != nil {
		t.Error("expected nil value")
	}
	if v, ok := lru.Get("key2"); !ok {
		t.Error("expected key2 to be in cache")
	} else if v != "value2" {
		t.Error("expected value2")
	}
	if v, ok := lru.Get("key3"); !ok {
		t.Error("expected key3 to be in cache")
	} else if v != "value3" {
		t.Error("expected value3")
	}

	lru.Delete("key2")
	if _, ok := lru.Get("key2"); ok {
		t.Error("expected key2 to be evicted")
	}
}
