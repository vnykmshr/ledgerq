# LedgerQ

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/vnykmshr/ledgerq.svg)](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq)

A persistent, disk-backed message queue for Go with no external dependencies.

## Features

- Crash-safe durability with append-only log design
- Zero dependencies beyond the Go standard library
- High throughput with batch operations
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

Benchmarks on Intel i5-8257U:

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
