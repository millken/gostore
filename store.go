package gostore

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/sync/singleflight"

	bolt "go.etcd.io/bbolt"
)

const (
	_fileMode          = 0600
	_defaultBucket     = "default"
	_defaultNumRetries = 3
)

var (
	// ErrNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrNotFound = errors.New("key not found")

	// ErrBadValue is returned when the value supplied to the Put method
	// is nil.
	ErrBadValue = errors.New("bad value")
)

// Option the tracer provider option
type Option func(*option) error

type option struct {
	numRetries   uint8
	readOnly     bool
	maxCacheSize int // maxCacheSize is the maximum number of items in the LRU cache.
}

// WithNumRetries defines service name
func WithNumRetries(n uint8) Option {
	return func(o *option) error {
		o.numRetries = n
		return nil
	}
}

// WithMaxCacheSize sets the maximum number of items in the LRU cache.
func WithMaxCacheSize(maxCacheSize int) Option {
	return func(o *option) error {
		o.maxCacheSize = maxCacheSize
		return nil
	}
}

// WithReadOnly set the store to read-only mode
func WithReadOnly() Option {
	return func(o *option) error {
		o.readOnly = true
		return nil
	}
}

// Store is KVStore implementation based bolt DB
type Store struct {
	opt   *option
	db    *bolt.DB
	lru   *lru
	group singleflight.Group
}

// Open opens a store with the given config
func Open(DbPath string, opts ...Option) (*Store, error) {
	var (
		err error
		opt option
		lru *lru
	)
	boltOpts := bolt.DefaultOptions
	for _, o := range opts {
		if err = o(&opt); err != nil {
			return nil, err
		}
	}
	if opt.numRetries == 0 {
		opt.numRetries = _defaultNumRetries
	}
	if opt.maxCacheSize > 0 {
		lru = newLRU(opt.maxCacheSize)
	}
	boltOpts.ReadOnly = opt.readOnly
	boltOpts.NoSync = true
	boltOpts.NoFreelistSync = true

	db, err := bolt.Open(DbPath, _fileMode, boltOpts)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:    db,
		opt:   &opt,
		lru:   lru,
		group: singleflight.Group{},
	}, nil
}

// Close closes the store
func (s *Store) Close() error {
	return s.db.Close()
}

// Put inserts a <key, value> record
func (s *Store) Put(namespace string, key, value []byte) (err error) {
	for c := uint8(0); c < s.opt.numRetries; c++ {
		if err = s.db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(namespace))
			if err != nil {
				return err
			}
			return bucket.Put(key, value)
		}); err == nil {
			break
		}
	}
	if err != nil {
		err = fmt.Errorf("failed to put key %s: %w", key, err)
	}

	return err
}

// Get fetches a value by key
func (s *Store) Get(namespace string, key []byte) ([]byte, error) {
	var value []byte
	var err error
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return ErrNotFound
		}
		value = bucket.Get(key)
		if value == nil {
			return ErrNotFound
		}
		return nil
	})
	return value, err
}

// Delete deletes a record by key
func (s *Store) Delete(namespace string, key []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return nil
		}
		return bucket.Delete(key)
	})
}

// Update set value by key, value must be gob-encodable
func (s *Store) Update(key string, value any) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	if err := s.Put(_defaultBucket, []byte(key), buf.Bytes()); err != nil {
		return err
	}
	s.tryAddToLRU(key, value)
	return nil
}

//Load read value by key
func (s *Store) Load(key string, obj any) error {
	if obj == nil {
		return ErrBadValue
	}
	if s.lru != nil {
		if v, ok := s.lru.Get(key); ok {
			return assign(obj, v)
		}
	}
	buf, err := s.Get(_defaultBucket, []byte(key))
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(obj); err != nil {
		return err
	}
	s.tryAddToLRU(key, obj)
	return nil
}

// Remove delete a record by key
func (s *Store) Remove(key string) error {
	if s.lru != nil {
		s.lru.Delete(key)
	}
	return s.Delete(_defaultBucket, []byte(key))
}

// Memoize memoize a function
func (s *Store) Memoize(key string, obj any, f func() (any, error)) error {
	if err := s.Load(key, obj); err != nil {
		if err != ErrNotFound {
			return err
		}

		value, err, _ := s.group.Do(key, func() (any, error) {
			data, innerErr := f()
			if innerErr != nil {
				return nil, innerErr
			}
			if err := s.Update(key, data); err != nil {
				return nil, err
			}
			return data, nil
		})
		if err != nil {
			return err
		}
		if err := assign(obj, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) tryAddToLRU(key string, value any) {
	if s.lru == nil {
		return
	}
	s.lru.Add(key, value)
}

func assign(dst, src any) error {
	if src == nil {
		return ErrBadValue
	}
	value := reflect.ValueOf(dst)
	if value.Kind() != reflect.Ptr {
		return errors.New("dst must be a pointer")
	}
	value.Elem().Set(reflect.ValueOf(src))
	return nil
}
