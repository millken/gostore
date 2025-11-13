[![codecov](https://codecov.io/gh/millken/gostore/branch/main/graph/badge.svg?token=kqm6VuhJ2C)](https://codecov.io/gh/millken/gostore)
[![Test status](https://github.com/millken/gostore/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/millken/gostore/actions?workflow=test)
[![Go Report Card](https://goreportcard.com/badge/github.com/millken/gostore)](https://goreportcard.com/report/github.com/millken/gostore)
[![GoDev](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/millken/gostore)
[![GitHub release](https://img.shields.io/github/release/millken/gostore.svg)](https://github.com/millken/gostore/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# gostore

gostore is a simple yet powerful key-value store written in Go that provides a fast access read interface through intelligent caching. Built on top of BBolt (a port of BoltDB to Go), it offers persistent storage with an optional LRU cache layer for high-performance read operations.

## Features

- **Persistent Storage**: Built on BBolt for reliable, ACID-compliant data persistence
- **High-Performance Caching**: Optional LRU cache with TTL support for fast reads
- **Namespace Support**: Organize data using buckets/namespaces
- **TTL Support**: Automatic expiration of cached and stored data
- **Memoization**: Cache function results with singleflight protection to prevent cache stampede
- **Type Safety**: Full support for Go types implementing `encoding.BinaryMarshaler`
- **Robust Error Handling**: Comprehensive input validation and clear error messages
- **Configurable**: Flexible configuration options for retries, cache size, and read-only mode

## Installation

```bash
go get github.com/millken/gostore
```

## Quick Start

### Basic Usage

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"

    "github.com/millken/gostore"
)

type User struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

func (u User) MarshalBinary() ([]byte, error) {
    return json.Marshal(u)
}

func (u *User) UnmarshalBinary(data []byte) error {
    return json.Unmarshal(data, u)
}

func main() {
    // Open a store with LRU cache
    store, err := gostore.Open("/tmp/mydb",
        gostore.WithMaxCacheSize(1000),
        gostore.WithNumRetries(3),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Store a user
    user := User{Name: "Alice", Age: 30}
    err = store.Update("user:1", &user)
    if err != nil {
        log.Fatal(err)
    }

    // Load the user
    var loadedUser User
    err = store.Load("user:1", &loadedUser)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Loaded user: %+v\n", loadedUser)
}
```

### In-Memory Store (for testing)

```go
package main

import (
    "fmt"
    "log"

    "github.com/millken/gostore"
)

func main() {
    // Create an in-memory store for testing
    store, err := gostore.OpenMemory(
        gostore.WithMaxCacheSize(1000),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close() // Automatically cleans up temp files

    // Use it exactly like a regular store
    err = store.Put("test", []byte("key"), []byte("value"))
    if err != nil {
        log.Fatal(err)
    }

    value, err := store.Get([]byte("test"), []byte("key"))
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Value: %s\n", string(value))
}
```

## API Reference

### Opening a Store

```go
// Basic file-based store
store, err := gostore.Open("/path/to/db")

// With options
store, err := gostore.Open("/path/to/db",
    gostore.WithMaxCacheSize(1000),     // LRU cache size
    gostore.WithNumRetries(5),          // Retry attempts for DB operations
    gostore.WithReadOnly(),             // Read-only mode
)

// In-memory store for testing
memoryStore, err := gostore.OpenMemory(
    gostore.WithMaxCacheSize(1000),     // LRU cache size
    gostore.WithNumRetries(3),          // Retry attempts
)
```

### Configuration Options

- `WithMaxCacheSize(size int)`: Set maximum number of items in LRU cache (0 = no cache)
- `WithNumRetries(n uint8)`: Set number of retry attempts (1-10, default: 3)
- `WithReadOnly()`: Open store in read-only mode

### Basic Operations

#### Put/Get Operations

```go
// Store raw bytes
err := store.Put("namespace", []byte("key"), []byte("value"))

// Store with TTL (seconds)
err := store.PutWithTTL([]byte("namespace"), []byte("key"), []byte("value"), 3600)

// Retrieve raw bytes
value, err := store.Get([]byte("namespace"), []byte("key"))

// Delete a key
err := store.Delete("namespace", []byte("key"))

// Delete entire namespace
err := store.DeleteNamespace("namespace")
```

#### Type-Safe Operations

```go
// Store any type implementing encoding.BinaryMarshaler
err := store.Update("user:1", &User{Name: "Bob"})

// Store with TTL
err := store.UpdateWithTTL("user:2", &User{Name: "Charlie"}, 3600)

// Load into any type implementing encoding.BinaryUnmarshaler
var user User
err := store.Load("user:1", &user)

// Remove a key
err := store.Remove("user:1")
```

### Memoization

Cache expensive function calls with automatic singleflight protection:

```go
var result Result
err := store.Memoize("expensive_operation", &result, func() (interface{}, error) {
    // This function will only be called once per key
    return performExpensiveOperation()
})

// With TTL
err := store.MemoizeWithTTL("cached_result", &result, func() (interface{}, error) {
    return fetchDataFromAPI()
}, 300) // 5 minutes TTL
```

## Error Handling

gostore provides clear, typed errors for different scenarios:

```go
var (
    gostore.ErrKeyNotFound     // Key doesn't exist
    gostore.ErrKeyExpired      // Key has expired
    gostore.ErrBadValue        // Invalid value (doesn't implement BinaryMarshaler)
    gostore.ErrInvalidInput    // Invalid input parameters
)
```

Example:

```go
import (
    "errors"
    "fmt"
    "log"
)

err := store.Load("nonexistent", &value)
if errors.Is(err, gostore.ErrKeyNotFound) {
    fmt.Println("Key does not exist")
} else if err != nil {
    log.Printf("Unexpected error: %v", err)
}
```

## Performance

gostore is optimized for high-performance scenarios:

- **Cached reads**: ~400ns per operation
- **Regular reads**: ~900ns per operation
- **Writes**: ~3.7μs per operation

Benchmarks show excellent performance with minimal memory allocations thanks to the efficient LRU cache implementation and BBolt's optimized storage engine.

## Use Cases

- **Configuration storage**: Store and cache application configuration
- **Session management**: Fast user session storage with automatic expiration
- **Caching layer**: Memoize expensive function calls and API responses
- **Metadata storage**: Store file metadata, indexes, or auxiliary data
- **Local caching**: Fast local cache for distributed applications

## Thread Safety

gostore is thread-safe for concurrent read operations. Write operations are serialized through BBolt's transaction system. The built-in singleflight mechanism ensures safe memoization across multiple goroutines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Development

```bash
# Run tests
go test -v ./...

# Run benchmarks
go test -bench=. -benchmem

# Run tests with race detector
go test -race -v ./...
```