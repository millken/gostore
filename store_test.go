package gostore

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path, WithNumRetries(1))
	if err != nil {
		t.Fatal(err)
	} else if s == nil {
		t.Fatal("expected db")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPut(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put("test", []byte("key"), []byte("value")); err != nil {
		t.Error(err)
	}
}

func TestFetch(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Put("test", []byte("key"), []byte("value")); err != nil {
		t.Error(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path, WithReadOnly())
	if err != nil {
		t.Fatal(err)
	}

	value, err := s.Get([]byte("test"), []byte("key"))
	if err != nil {
		t.Error(err)
	}
	if string(value) != "value" {
		t.Errorf("expected value %s, got %s", "value", value)
	}
}

func TestOptionWithMaxCacheSize(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path, WithMaxCacheSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v T1
	if err := s.Load("test", &v); err != ErrKeyNotFound {
		t.Errorf("expected error %s, got %s", ErrKeyNotFound, err)
	}
	if err := s.Update("test", &T1{Name: "value"}); err != nil {
		t.Error(err)
	}
	err = s.Load("test", &v)
	if err != nil {
		t.Error(err)
	}
	if v.Name != "value" {
		t.Errorf("expected value %s, got %s", "value", v.Name)
	}
}

func TestFetchNotFound(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Get([]byte("test"), []byte("key"))
	if err != ErrKeyNotFound {
		t.Errorf("expected error %s, got %s", ErrKeyNotFound, err)
	}
}

func TestDelete(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put("test", []byte("key"), []byte("value")); err != nil {
		t.Error(err)
	}

	if err := s.Delete("test", []byte("key")); err != nil {
		t.Error(err)
	}

	_, err = s.Get([]byte("test"), []byte("key"))
	if err != ErrKeyNotFound {
		t.Errorf("expected error %s, got %s", ErrKeyNotFound, err)
	}
}

func TestDeleteNamespace(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Put("test", []byte("a"), []byte("aa")); err != nil {
		t.Error(err)
	}
	if err := s.DeleteNamespace("test"); err != nil {
		t.Error(err)
	}
	s.Close()

}

func TestUpdateLoadRemove(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	value := &T1{Name: "test"}
	if err := s.Update("test", value); err != nil {
		t.Error(err)
	}
	s.Close()

	s, _ = Open(path, WithMaxCacheSize(1))
	var v T1
	if err := s.Load("test", &v); err != nil {
		t.Error(err)
	}
	if v.Name != "test" {
		t.Errorf("expected value %s, got %s", "test", v.Name)
	}
	if err := s.Remove("test"); err != nil {
		t.Error(err)
	}
	if err := s.Load("test", &v); err != ErrKeyNotFound {
		t.Errorf("expected error %s, got %s", ErrKeyNotFound, err)
	}
}

func TestUpdateLoadRemoveNotFound(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var v T1
	if err := s.Load("test", &v); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected error %s, got %s", ErrKeyNotFound, err)
	}
	if err := s.Remove("test"); err != nil {
		t.Error(err)
	}
}

type T1 struct {
	Name string `json:"name"`
	Uid  int    `json:"uid"`
}

func (t T1) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *T1) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func TestMemoize(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	v := &T1{}
	if err := s.Memoize("test", v, func() (any, error) {
		t.Log("loading from db")
		v1 := &T1{Name: "test"}
		return v1, nil
	}); err != nil {
		t.Error(err)
	}
	if v.Name != "test" {
		t.Errorf("expected value %s, got %s", "test", v.Name)
	}
	// second call should load from cache
	v2 := &T1{}
	if err := s.Memoize("test", v2, func() (any, error) {
		t.Log("loading from cache")
		return nil, errors.New("should not be called")
	}); err != nil {
		t.Error(err)
	}
	if v2.Name != v.Name {
		t.Errorf("expected value %s, got %s", "test", v.Name)
	}
}

func TestMemoizeWithTTL(t *testing.T) {
	path, err := tempfile()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	v := &T1{}
	if err := s.MemoizeWithTTL("test", v, func() (any, error) {
		t.Log("loading from db")
		v1 := &T1{Name: "test"}
		return v1, nil
	}, 2); err != nil {
		t.Error(err)
	}
	if v.Name != "test" {
		t.Errorf("expected value %s, got %s", "test", v.Name)
	}
	// second call should load from cache
	v2 := &T1{}
	if err := s.MemoizeWithTTL("test", v2, func() (any, error) {
		t.Log("loading from cache")
		return nil, errors.New("should not be called")
	}, 2); err != nil {
		t.Error(err)
	}
	if v2.Name != v.Name {
		t.Errorf("expected value %s, got %s", "test", v.Name)
	}
	// wait for ttl to expire
	time.Sleep(3 * time.Second)
	// third call should load from db
	v3 := &T1{}
	if err := s.MemoizeWithTTL("test", v3, func() (any, error) {
		v1 := &T1{Name: "test"}
		return v1, nil
	}, 2); err != nil {
		t.Error(err)
	}

}

func BenchmarkStoreWithCache(b *testing.B) {
	path, err := tempfile()
	if err != nil {
		b.Error(err)
	}
	defer os.RemoveAll(path)
	s, err := Open(path, WithMaxCacheSize(100))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	value := &T1{Name: "test"}

	b.Run("Update", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := s.Update("test", value); err != nil {
				b.Error(err)
			}
		}
	})
	b.Run("Load", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var v T1
			if err := s.Load("test", &v); err != nil {
				b.Error(err)
			}
		}
	})
}

func tempfile() (string, error) {
	tempFile, err := os.CreateTemp(os.TempDir(), "store_test")
	if err != nil {
		return "", err
	}
	return tempFile.Name(), tempFile.Close()

}
