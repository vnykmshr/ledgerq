# LedgerQ

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**LedgerQ** is a persistent, disk-backed message queue written in Go. It provides durable message storage with FIFO guarantees, automatic segment rotation, and efficient replay capabilities—all without external dependencies.

## Why LedgerQ?

Go developers often need a simple, reliable queue for embedded or local-first applications—something lighter than Kafka or NSQ, but more durable than in-memory channels. LedgerQ fills that gap by providing:

- **Zero dependencies** beyond the Go standard library
- **Crash-safe durability** with append-only log design
- **Batch operations** for high throughput
- **Replay capability** from message ID or timestamp
- **Auto-compaction** with retention policies
- **Thread-safe** concurrent access
- **Pluggable logging** for observability

## Features

- ✅ **Persistent Storage** — All messages durably written to disk
- ✅ **FIFO Ordering** — Strict first-in-first-out guarantees
- ✅ **Read Position Persistence** — Consumer offset tracked across restarts
- ✅ **Automatic Segment Rotation** — Configurable by size, count, time, or both
- ✅ **Batch Operations** — Efficient EnqueueBatch/DequeueBatch with single fsync
- ✅ **Replay Capabilities** — Seek by message ID or timestamp
- ✅ **Compaction & Retention** — Automatic background or manual cleanup
- ✅ **Thread-Safe** — Safe for concurrent producers and consumers
- ✅ **Sparse Indexing** — Fast lookups with minimal overhead
- ✅ **Pluggable Logging** — Optional structured logging
- ✅ **Pure Go** — No C dependencies or external tools required

## Installation

```bash
go get github.com/vnykmshr/ledgerq
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/vnykmshr/ledgerq/internal/queue"
)

func main() {
    // Open a queue
    q, err := queue.Open("/path/to/queue", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // Enqueue a message
    offset, err := q.Enqueue([]byte("Hello, World!"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Enqueued at offset: %d\n", offset)

    // Dequeue a message
    msg, err := q.Dequeue()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Dequeued [ID:%d]: %s\n", msg.ID, msg.Payload)
}
```

## Usage

### Basic Operations

#### Opening a Queue

```go
// Use default options
q, err := queue.Open("/path/to/queue", nil)

// Or configure options
opts := queue.DefaultOptions("/path/to/queue")
opts.AutoSync = true  // Sync after every write
opts.SyncInterval = 1 * time.Second
q, err := queue.Open("/path/to/queue", opts)
```

#### Enqueuing Messages

```go
// Single message
offset, err := q.Enqueue([]byte("message"))

// Batch (more efficient - single fsync)
payloads := [][]byte{
    []byte("message1"),
    []byte("message2"),
    []byte("message3"),
}
offsets, err := q.EnqueueBatch(payloads)
```

#### Dequeuing Messages

```go
// Single message
msg, err := q.Dequeue()
if err != nil {
    // No messages available
}
fmt.Printf("ID: %d, Payload: %s\n", msg.ID, msg.Payload)

// Batch
messages, err := q.DequeueBatch(10) // Read up to 10 messages
```

### Advanced Features

#### Replay Operations

```go
// Seek to specific message ID
err := q.SeekToMessageID(100)

// Seek to first message at or after timestamp
timestamp := time.Now().Add(-1 * time.Hour).UnixNano()
err := q.SeekToTimestamp(timestamp)

// Now dequeue from that position
msg, err := q.Dequeue()
```

#### Segment Management

```go
import "github.com/vnykmshr/ledgerq/internal/segment"

opts := queue.DefaultOptions("/path/to/queue")

// Configure rotation policy
opts.SegmentOptions.RotationPolicy = segment.RotateBySize
opts.SegmentOptions.MaxSegmentSize = 100 * 1024 * 1024 // 100MB

// Or rotate by message count
opts.SegmentOptions.RotationPolicy = segment.RotateByCount
opts.SegmentOptions.MaxSegmentMessages = 1000000

// Or both (rotate when either limit is reached)
opts.SegmentOptions.RotationPolicy = segment.RotateByBoth
```

#### Compaction and Retention

```go
opts := queue.DefaultOptions("/path/to/queue")

// Configure retention policy
opts.SegmentOptions.RetentionPolicy = &segment.RetentionPolicy{
    MaxAge:      7 * 24 * time.Hour, // Keep 7 days
    MaxSize:     10 * 1024 * 1024 * 1024, // Max 10GB total
    MaxSegments: 100,  // Keep max 100 segments
    MinSegments: 1,    // Always keep at least 1
}

// Enable automatic background compaction (optional)
opts.CompactionInterval = 5 * time.Minute // Run every 5 minutes

q, _ := queue.Open("/path/to/queue", opts)

// Or manually trigger compaction anytime
result, err := q.segments.Compact()
fmt.Printf("Removed %d segments, freed %d bytes\n",
    result.SegmentsRemoved, result.BytesFreed)
```

#### Logging

```go
import "github.com/vnykmshr/ledgerq/internal/logging"

opts := queue.DefaultOptions("/path/to/queue")

// Use default logger
opts.Logger = logging.NewDefaultLogger(logging.LevelInfo)

// Or implement your own
type MyLogger struct{}

func (l MyLogger) Debug(msg string, fields ...logging.Field) { /* ... */ }
func (l MyLogger) Info(msg string, fields ...logging.Field)  { /* ... */ }
func (l MyLogger) Warn(msg string, fields ...logging.Field)  { /* ... */ }
func (l MyLogger) Error(msg string, fields ...logging.Field) { /* ... */ }

opts.Logger = MyLogger{}
```

#### Queue Statistics

```go
stats := q.Stats()
fmt.Printf("Total messages: %d\n", stats.TotalMessages)
fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
fmt.Printf("Segments: %d\n", stats.SegmentCount)
fmt.Printf("Next message ID: %d\n", stats.NextMessageID)
fmt.Printf("Read position: %d\n", stats.ReadMessageID)
```

## Examples

See the [examples](examples/) directory for complete, runnable examples:

- **[simple](examples/simple)**: Basic queue operations
- **[producer-consumer](examples/producer-consumer)**: Multi-producer, multi-consumer pattern
- **[replay](examples/replay)**: Replay and seeking capabilities

Run any example:
```bash
go run examples/simple/main.go
go run examples/producer-consumer/main.go
go run examples/replay/main.go
```

## Architecture

### Storage Format

LedgerQ uses a log-structured design with segment-based storage:

**Segments**: Messages stored in append-only `.log` files with companion `.idx` index files
**Naming**: Zero-padded base offsets like `00000000000000001000.log`
**Rotation**: Automatic based on size, count, or time policies

**Entry Format** (26 bytes + payload):
- Type (1B) + Flags (1B) + Length (4B) + Message ID (8B) + Timestamp (8B) + Payload (variable) + CRC32 (4B)

**Index Format** (sparse, every 4KB):
- Message ID (8B) + Timestamp (8B) + File Offset (8B)

### Performance

Benchmarks on modern hardware (Intel i5-8257U):

```
Single Operations (without sync):
  Enqueue:  ~300-400 ns/op
  Dequeue:  ~700 μs/op (with disk read)

Batch Operations (10 messages):
  EnqueueBatch: ~2 μs/op (~200 ns per message)
  DequeueBatch: ~7 μs/op (~700 ns per message)

With AutoSync Enabled:
  Enqueue:  ~19 ms/op (includes fsync)
  EnqueueBatch (100): ~20 ms total (~200 μs per message)

Concurrent (8 writers):
  Enqueue:  ~350 ns/op (maintains performance)
  Batch:    ~2.2 μs/op (excellent scalability)
```

**Key Takeaway**: Batching provides 10-100x performance improvement.

## Design Decisions

### Segment-Based Storage

- **Efficient Compaction**: Remove old data without rewriting entire queue
- **Bounded File Sizes**: Avoid single files growing too large
- **Fast Startup**: Only scan last segment on recovery
- **Better Caching**: OS can cache hot segments efficiently

### Read Position Persistence

Read position is automatically persisted to disk. After reopening, queues continue from where they left off. This enables true queue semantics where consumed messages remain consumed across restarts.

- **Automatic**: Position updates on every dequeue operation
- **Crash-Safe**: Metadata synced according to AutoSync option
- **Recovery**: Hybrid approach using both metadata and segment scanning
- **Seekable**: Explicit seeking updates persisted position

### Sparse Indexing

- **Space Efficient**: Index entries only every 4KB
- **Fast Enough**: Sequential scan of 4KB is negligible
- **Simple**: No complex index maintenance

## Testing

```bash
# Run all tests
go test ./...

# With race detector
go test -race ./...

# Integration tests
go test -v ./internal/queue -run TestIntegration

# Benchmarks
go test -bench=. ./internal/queue

# Specific benchmark
go test -bench=BenchmarkConcurrent ./internal/queue
```

## Project Status

LedgerQ is feature-complete and suitable for:
- Event sourcing applications
- Audit logging
- Local message buffering
- Development and testing
- Learning about message queue internals

**Not Recommended For**:
- High-throughput distributed systems (use Kafka, NATS, etc.)
- Multi-node scenarios (single-node only)
- Consumer groups or multiple independent consumers

## Use Cases

- **Offline task queue**: Buffer background jobs locally
- **Telemetry/event buffer**: Persist analytics offline
- **Local event sourcing**: Log domain events with replay
- **Edge/IoT queue**: Message persistence in constrained environments
- **Job replay testing**: Reproduce job batches deterministically

## Contributing

This is an educational project. Feel free to fork, experiment, and learn!

## License

Apache License 2.0 - see LICENSE file for details.

## Acknowledgments

Inspired by:
- Apache Kafka's log-structured storage
- NATS Streaming's message persistence
- Python's [persist-queue](https://github.com/peter-wangxu/persist-queue)
- Various academic papers on log-structured systems

Built as a learning project to understand message queue internals and Go systems programming.
