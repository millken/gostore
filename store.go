package gostore

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"

	bolt "go.etcd.io/bbolt"
)

const (
	_fileMode          = 0600
	_defaultBucket     = "default"
	_bucketTTL         = "ttl"
	_defaultNumRetries = 3
)

var (
	// ErrKeyNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyExpired is returned when the key supplied to a Get or Delete
	ErrKeyExpired = errors.New("key expired")

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

type valueT struct {
	Value  []byte
	Expire time.Time
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
	return s.PutWithTTL([]byte(namespace), key, value, 0)
}

// PutWithTTL inserts a <key, value> record with TTL
func (s *Store) PutWithTTL(namespace, key, value []byte, ttl int64) (err error) {
	for c := uint8(0); c < s.opt.numRetries; c++ {
		if err = s.db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(namespace)
			if err != nil {
				return err
			}
			newvalue := bytesToValue(value, ttl)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(newvalue); err != nil {
				return err
			}

			return bucket.Put(key, buf.Bytes())
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
func (s *Store) Get(namespace, key []byte) ([]byte, error) {
	valT, err := s.get(namespace, key)
	if err != nil {
		return nil, err
	}
	if valT.Expire.IsZero() {
		return valT.Value, nil
	}
	if time.Now().After(valT.Expire) {
		return nil, ErrKeyExpired
	}
	return valT.Value, err
}

func (s *Store) get(namespace, key []byte) (valueT, error) {
	var value valueT
	var err error
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(namespace)
		if bucket == nil {
			return ErrKeyNotFound
		}
		val := bucket.Get(key)
		if val == nil {
			return ErrKeyNotFound
		}

		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&value); err != nil {
			return err
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
	return s.UpdateWithTTL(key, value, 0)
}

// UpdateWithTTL set value by key with TTL, value must be gob-encodable
func (s *Store) UpdateWithTTL(key string, value any, ttl int64) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	if err := s.PutWithTTL([]byte(_defaultBucket), []byte(key), buf.Bytes(), ttl); err != nil {
		return err
	}
	s.tryAddToLRU(key, value, ttl)
	return nil
}

func bytesToValue(value []byte, ttl int64) valueT {
	newvalue := valueT{
		Value: value,
	}
	if ttl == 0 {
		newvalue.Expire = time.Time{}
	} else {
		newvalue.Expire = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	return newvalue
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
	valT, err := s.get([]byte(_defaultBucket), []byte(key))
	if err != nil {
		return err
	}
	now := time.Now()
	if !valT.Expire.IsZero() && now.After(valT.Expire) {
		return ErrKeyExpired
	}
	if err := gob.NewDecoder(bytes.NewReader(valT.Value)).Decode(obj); err != nil {
		return err
	}
	s.tryAddToLRU(key, obj, int64(valT.Expire.Sub(now).Seconds()))
	return nil
}

// DeleteNamespace deletes a namespace
func (s *Store) DeleteNamespace(namespace string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(namespace))
	})
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
	return s.MemoizeWithTTL(key, obj, f, 0)
}

func (s *Store) MemoizeWithTTL(key string, obj any, f func() (any, error), ttl int64) error {
	if err := s.Load(key, obj); err != nil {
		if err != ErrKeyNotFound && err != ErrKeyExpired {
			return err
		}

		value, err, _ := s.group.Do(key, func() (any, error) {
			data, innerErr := f()
			if innerErr != nil {
				return nil, innerErr
			}
			if err := s.UpdateWithTTL(key, data, ttl); err != nil {
				return nil, err
			}
			return data, nil
		})
		if err != nil {
			return err
		}
		return assign(obj, value)
	}
	return nil
}

func (s *Store) tryAddToLRU(key string, value any, ttl int64) {
	if s.lru == nil {
		return
	}
	expire := time.Time{}
	if ttl > 0 {
		expire = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	s.lru.Add(key, expire, value)
}
