# LedgerQ

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/vnykmshr/ledgerq.svg)](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq)

A lightweight, persistent, disk-backed message queue for Go. Zero dependencies, crash-safe, and designed for embedded and local-first applications.

## Features

- 🔒 **Crash-safe durability** with append-only log design
- 📦 **Zero dependencies** beyond the Go standard library
- 🚀 **High throughput** with batch operations
- 🔄 **Replay capability** from message ID or timestamp
- ⏰ **Message TTL** for automatic expiration
- 🏷️ **Message headers** for metadata and routing
- 📊 **Metrics & logging** for observability
- 🧹 **Auto-compaction** with retention policies
- 🔀 **Thread-safe** for concurrent access
- 🛠️ **CLI tool** for queue management

## Why LedgerQ?

Go developers often need a simple, reliable queue for embedded or local-first applications—something lighter than Kafka or NSQ, but more durable than in-memory channels. LedgerQ fills that gap.

**Ideal for**:
- Offline task queues
- Event sourcing and audit logs
- Local message buffering
- Edge/IoT applications
- Development and testing

**Not for**:
- High-throughput distributed systems (use Kafka, NATS)
- Multi-node scenarios (single-node only)
- Multiple independent consumers (use separate queues)

## Quick Start

### Installation

```bash
go get github.com/vnykmshr/ledgerq
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/vnykmshr/ledgerq/pkg/ledgerq"
)

func main() {
    // Open a queue
    q, err := ledgerq.Open("/tmp/myqueue", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // Enqueue a message
    offset, _ := q.Enqueue([]byte("Hello, World!"))
    fmt.Printf("Enqueued at offset: %d\n", offset)

    // Dequeue a message
    msg, _ := q.Dequeue()
    fmt.Printf("Dequeued [ID:%d]: %s\n", msg.ID, msg.Payload)
}
```

### Configuration

```go
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.AutoSync = true                       // fsync after every write
opts.MaxSegmentSize = 100 * 1024 * 1024   // 100MB segments
opts.CompactionInterval = 5 * time.Minute  // Auto-compact every 5 min

q, _ := ledgerq.Open("/tmp/myqueue", opts)
```

## Core Operations

### Batch Operations (10-100x faster)

```go
// Enqueue batch
payloads := [][]byte{
    []byte("msg1"),
    []byte("msg2"),
    []byte("msg3"),
}
offsets, _ := q.EnqueueBatch(payloads)

// Dequeue batch
messages, _ := q.DequeueBatch(10) // up to 10 messages
```

### Message TTL

```go
// Expire after 5 seconds
q.EnqueueWithTTL([]byte("temporary"), 5*time.Second)

// Dequeue automatically skips expired messages
msg, _ := q.Dequeue()
```

### Message Headers

```go
headers := map[string]string{
    "content-type":   "application/json",
    "correlation-id": "req-12345",
}
q.EnqueueWithHeaders(payload, headers)
```

### Streaming API

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

q.Stream(ctx, func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    return nil
})
```

### Replay

```go
// Seek to message ID
q.SeekToMessageID(100)

// Seek to timestamp
oneHourAgo := time.Now().Add(-1 * time.Hour).UnixNano()
q.SeekToTimestamp(oneHourAgo)
```

## Performance

Benchmarks on modern hardware (Intel i5-8257U):

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| Enqueue (no sync) | ~3M ops/sec | 300-400 ns | Buffered |
| Enqueue (AutoSync) | ~50 ops/sec | ~19 ms | With fsync |
| EnqueueBatch 100 | ~50M msgs/sec | 200 ns/msg | Single fsync |
| Dequeue | ~1.4K ops/sec | ~700 μs | With disk read |

**Key takeaway**: Use batch operations for high throughput.

## Examples

See the [examples/](examples/) directory for complete, runnable examples:

- **[simple](examples/simple)** - Basic queue operations
- **[producer-consumer](examples/producer-consumer)** - Multi-goroutine pattern
- **[streaming](examples/streaming)** - Real-time streaming API
- **[ttl](examples/ttl)** - Message expiration
- **[headers](examples/headers)** - Message metadata
- **[replay](examples/replay)** - Seeking and replay
- **[metrics](examples/metrics)** - Metrics collection

Run any example:
```bash
go run examples/simple/main.go
```

## CLI Tool

Install:
```bash
go install github.com/vnykmshr/ledgerq/cmd/ledgerq@latest
```

Usage:
```bash
# Show statistics
ledgerq stats /path/to/queue

# Inspect queue (JSON)
ledgerq inspect /path/to/queue

# Compact manually
ledgerq compact /path/to/queue

# Peek messages (without consuming)
ledgerq peek /path/to/queue 5
```

## Documentation

- **[Usage Guide](docs/USAGE.md)** - Comprehensive usage documentation
- **[Architecture](docs/ARCHITECTURE.md)** - Internal design and implementation
- **[API Reference](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq)** - GoDoc documentation
- **[Contributing](CONTRIBUTING.md)** - Development guidelines

## Testing

```bash
# Run all tests
go test ./...

# With race detector
go test -race ./...

# Benchmarks
go test -bench=. ./internal/queue

# Fuzzing (Go 1.18+)
go test -fuzz=FuzzEnqueueDequeue -fuzztime=30s ./internal/queue
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Inspired by:
- Apache Kafka's log-structured storage
- NATS Streaming's message persistence
- Python's [persist-queue](https://github.com/peter-wangxu/persist-queue)

Built as a learning project to understand message queue internals and Go systems programming.

## Project Status

**v1.0.0** - Feature-complete and production-ready for single-node use cases.

See [CHANGELOG.md](CHANGELOG.md) for version history and [SECURITY.md](SECURITY.md) for security policy.
