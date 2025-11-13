package gostore

import (
	"testing"
)

func TestOpenMemory(t *testing.T) {
	// Test basic memory store
	store, err := OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory store: %v", err)
	}
	defer store.Close()

	// Test basic operations
	if err := store.Put("test", []byte("key"), []byte("value")); err != nil {
		t.Errorf("Failed to put value: %v", err)
	}

	value, err := store.Get([]byte("test"), []byte("key"))
	if err != nil {
		t.Errorf("Failed to get value: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("Expected 'value', got '%s'", string(value))
	}

	// Test with cache
	cachedStore, err := OpenMemory(WithMaxCacheSize(100))
	if err != nil {
		t.Fatalf("Failed to open cached memory store: %v", err)
	}
	defer cachedStore.Close()

	user := &T1{Name: "Alice", Uid: 123}
	if err := cachedStore.Update("user", user); err != nil {
		t.Errorf("Failed to update user: %v", err)
	}

	var loadedUser T1
	if err := cachedStore.Load("user", &loadedUser); err != nil {
		t.Errorf("Failed to load user: %v", err)
	}
	if loadedUser.Name != "Alice" || loadedUser.Uid != 123 {
		t.Errorf("Expected Alice with Uid 123, got %+v", loadedUser)
	}
}

func TestOpenMemoryWithValidation(t *testing.T) {
	// Test invalid options
	if _, err := OpenMemory(WithNumRetries(0)); err == nil {
		t.Error("Expected error for 0 retries")
	}

	if _, err := OpenMemory(WithMaxCacheSize(-1)); err == nil {
		t.Error("Expected error for negative cache size")
	}

	// Test valid options
	store, err := OpenMemory(WithNumRetries(5), WithMaxCacheSize(50))
	if err != nil {
		t.Fatalf("Failed to open memory store with valid options: %v", err)
	}
	defer store.Close()
}

func TestMemoryStoreCleanup(t *testing.T) {
	store, err := OpenMemory()
	if err != nil {
		t.Fatalf("Failed to open memory store: %v", err)
	}

	// Store some data
	if err := store.Put("test", []byte("key"), []byte("value")); err != nil {
		t.Errorf("Failed to put value: %v", err)
	}

	// Close should clean up temp file
	if err := store.Close(); err != nil {
		t.Errorf("Failed to close memory store: %v", err)
	}
}

func BenchmarkMemoryStore(b *testing.B) {
	store, err := OpenMemory(WithMaxCacheSize(100))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	value := &T1{Name: "test"}

	b.Run("Update", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := store.Update("test", value); err != nil {
				b.Error(err)
			}
		}
	})

	b.Run("Load", func(b *testing.B) {
		// Pre-populate
		store.Update("test", value)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var v T1
			if err := store.Load("test", &v); err != nil {
				b.Error(err)
			}
		}
	})
}