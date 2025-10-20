# LedgerQ

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/vnykmshr/ledgerq.svg)](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq)

A disk-backed message queue with segment-based storage, TTL support, and priority ordering - written in pure Go with zero dependencies.

## Features

- Crash-safe durability with append-only log design
- Zero dependencies beyond the Go standard library
- High throughput with batch operations
- **Priority queue support (v1.1.0+)** with starvation prevention
- **Dead Letter Queue (DLQ) support (v1.2.0+)** for failed message handling
- **Payload compression (v1.3.0+)** with GZIP to reduce disk usage
- Replay from message ID or timestamp
- Message TTL and headers
- Metrics and pluggable logging
- Automatic compaction with retention policies
- Thread-safe concurrent access
- CLI tool for management

## Why LedgerQ?

For embedded or local-first applications that need something lighter than Kafka or NSQ, but more durable than in-memory channels.

**Good for:**
- Offline task queues
- Event sourcing and audit logs
- Local message buffering
- Edge/IoT applications

**Not for:**
- Distributed systems (use Kafka, NATS)
- Multi-node deployments
- Multiple independent consumers

## Quick Start

### Installation

```bash
go get github.com/vnykmshr/ledgerq/pkg/ledgerq@latest
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
    q, err := ledgerq.Open("/tmp/myqueue", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    offset, _ := q.Enqueue([]byte("Hello, World!"))
    fmt.Printf("Enqueued at offset: %d\n", offset)

    msg, _ := q.Dequeue()
    fmt.Printf("Dequeued [ID:%d]: %s\n", msg.ID, msg.Payload)
}
```

### Configuration

```go
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.AutoSync = true
opts.MaxSegmentSize = 100 * 1024 * 1024
opts.CompactionInterval = 5 * time.Minute

q, _ := ledgerq.Open("/tmp/myqueue", opts)
```

## Core Operations

**Batch operations** (10-100x faster):

```go
payloads := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3")}
offsets, _ := q.EnqueueBatch(payloads)

messages, _ := q.DequeueBatch(10)
```

**Message TTL:**

```go
q.EnqueueWithTTL([]byte("temporary"), 5*time.Second)
```

**Message headers:**

```go
headers := map[string]string{"content-type": "application/json"}
q.EnqueueWithHeaders(payload, headers)
```

**Payload compression (v1.3.0+):**

```go
// Enable compression by default
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.DefaultCompression = ledgerq.CompressionGzip
opts.CompressionLevel = 6  // 1 (fastest) to 9 (best compression)
opts.MinCompressionSize = 1024  // Only compress >= 1KB
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Messages are automatically compressed/decompressed
q.Enqueue(largePayload)  // Compressed if >= 1KB

// Or control compression per-message
q.EnqueueWithCompression(payload, ledgerq.CompressionGzip)
```

**Priority queue (v1.1.0+):**

```go
// Enable priority mode
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.EnablePriorities = true
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Enqueue with priority
q.EnqueueWithPriority([]byte("urgent"), ledgerq.PriorityHigh)
q.EnqueueWithPriority([]byte("normal"), ledgerq.PriorityMedium)
q.EnqueueWithPriority([]byte("background"), ledgerq.PriorityLow)

// Dequeue in priority order (High → Medium → Low)
msg, _ := q.Dequeue() // Returns "urgent" first
```

**Batch operations with priorities (v1.1.0+):**

```go
// Batch enqueue with per-message options
messages := []ledgerq.BatchEnqueueOptions{
    {Payload: []byte("urgent task"), Priority: ledgerq.PriorityHigh},
    {Payload: []byte("normal task"), Priority: ledgerq.PriorityMedium},
    {Payload: []byte("background task"), Priority: ledgerq.PriorityLow, TTL: time.Hour},
}
offsets, _ := q.EnqueueBatchWithOptions(messages)
```

**Dead Letter Queue (v1.2.0+):**

```go
// Enable DLQ with max retries
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.DLQPath = "/tmp/myqueue/dlq"
opts.MaxRetries = 3
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Process messages with Ack/Nack
msg, _ := q.Dequeue()

// Check retry info for custom backoff logic
if info := q.GetRetryInfo(msg.ID); info != nil {
    backoff := time.Duration(1<<uint(info.RetryCount)) * time.Second
    time.Sleep(backoff) // Exponential backoff
}

if processSuccessfully(msg.Payload) {
    q.Ack(msg.ID) // Mark as successfully processed
} else {
    q.Nack(msg.ID, "processing failed") // Record failure (moves to DLQ after MaxRetries)
}

// Inspect DLQ messages
dlq := q.GetDLQ()
dlqMsg, _ := dlq.Dequeue()
fmt.Printf("Failed: %s, Reason: %s\n",
    string(dlqMsg.Payload),
    dlqMsg.Headers["dlq.failure_reason"])

// Requeue from DLQ after fixing issues
q.RequeueFromDLQ(dlqMsg.ID)
```

**Note**: `Nack()` tracks failures but does not auto-retry messages. After `MaxRetries` failures, messages move to DLQ. For retry patterns with backoff, see [USAGE.md](docs/USAGE.md#dead-letter-queue-dlq---v120).

**Streaming:**

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

q.Stream(ctx, func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    return nil
})
```

**Replay:**

```go
q.SeekToMessageID(100)
q.SeekToTimestamp(time.Now().Add(-1 * time.Hour).UnixNano())
```

## Performance

Benchmarks (Go 1.21, macOS, Intel i5-8257U @ 2.3GHz):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Enqueue (buffered) | 3M ops/sec | 300-400 ns |
| Enqueue (fsync) | 50 ops/sec | 19 ms |
| EnqueueBatch (100 msgs) | 50M msgs/sec | 200 ns/msg |
| Dequeue | 1.4K ops/sec | 700 μs |

Use batch operations for high throughput.

## Examples

See [examples/](examples/) for runnable code:

- [simple](examples/simple) - Basic operations
- [producer-consumer](examples/producer-consumer) - Multiple goroutines
- [streaming](examples/streaming) - Streaming API
- [ttl](examples/ttl) - Message expiration
- [headers](examples/headers) - Message metadata
- [replay](examples/replay) - Seeking and replay
- [metrics](examples/metrics) - Metrics collection
- **[priority](examples/priority) - Priority queue (v1.1.0+)**
- **[dlq](examples/dlq) - Dead Letter Queue (v1.2.0+)**

## CLI Tool

```bash
go install github.com/vnykmshr/ledgerq/cmd/ledgerq@latest

ledgerq stats /path/to/queue
ledgerq inspect /path/to/queue
ledgerq compact /path/to/queue
ledgerq peek /path/to/queue 5
```

## Documentation

- [Usage Guide](docs/USAGE.md) - Complete reference
- [Architecture](docs/ARCHITECTURE.md) - Internal design
- [API Reference](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq) - GoDoc
- [Contributing](CONTRIBUTING.md) - Development guide

## Limitations

### Batch Operations and Priority Queues

**Important**: `DequeueBatch()` always returns messages in FIFO order (by message ID), even when `EnablePriorities=true`. This is a performance trade-off: batch dequeue optimizes for sequential I/O rather than priority ordering.

For priority-aware consumption, use `Dequeue()` in a loop or the `Stream()` API.

```go
// FIFO batch dequeue (fast, sequential I/O)
messages, _ := q.DequeueBatch(100)

// Priority-aware dequeue (respects priority order)
for i := 0; i < 100; i++ {
    msg, err := q.Dequeue()
    // ...
}
```

### Priority Queue Memory Usage

When `EnablePriorities=true`, the queue maintains an in-memory index of all unprocessed message IDs, organized by priority level. Memory usage scales linearly with queue depth (measured with runtime.MemStats):

- 1M pending messages: ~24 MB
- 10M pending messages: ~240 MB
- 100M pending messages: ~2.4 GB

For very deep queues (>10M messages), consider:
- Using FIFO mode (`EnablePriorities=false`)
- Implementing application-level batching to keep queue depth low
- Running compaction regularly to remove processed messages

## Testing

```bash
go test ./...
go test -race ./...
go test -bench=. ./internal/queue
go test -fuzz=FuzzEnqueueDequeue -fuzztime=30s ./internal/queue
```

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Credits

Inspired by Apache Kafka, NATS Streaming, and Python's persist-queue.

Built as a learning project for message queue internals and Go systems programming.
