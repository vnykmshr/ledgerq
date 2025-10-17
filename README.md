# LedgerQ

[![CI](https://github.com/vnykmshr/ledgerq/workflows/CI/badge.svg)](https://github.com/vnykmshr/ledgerq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/vnykmshr/ledgerq)](https://goreportcard.com/report/github.com/vnykmshr/ledgerq)
[![codecov](https://codecov.io/gh/vnykmshr/ledgerq/branch/main/graph/badge.svg)](https://codecov.io/gh/vnykmshr/ledgerq)
[![Go Reference](https://pkg.go.dev/badge/github.com/vnykmshr/ledgerq.svg)](https://pkg.go.dev/github.com/vnykmshr/ledgerq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**LedgerQ** is a lightweight, durable, file-backed queue for Go. Designed for embedded and local-first applications, it guarantees message persistence, safe recovery, and flexible replay—without external dependencies.

## Why LedgerQ?

Go developers often need a simple, reliable queue for offline or embedded use cases—something lighter than Kafka or NSQ, but more durable than in-memory channels. LedgerQ fills that gap by providing:

- **Zero dependencies** beyond the Go standard library
- **Crash-safe durability** with append-only ledger design
- **Batch operations** for efficient throughput
- **Replay capability** from offset, timestamp, or checkpoint
- **Auto-compaction** to reclaim disk space
- **Built-in observability** with metrics and introspection

Inspired by Python's `persist-queue`, LedgerQ brings the same simplicity to Go while adding batching, replay controls, and production-grade reliability.

## Features

- ✅ **File-backed persistence** — Messages survive process restarts and crashes
- ✅ **Batching support** — Efficient group enqueue/dequeue operations
- ✅ **Replay controls** — Replay from offset, timestamp, or user checkpoints
- ✅ **Auto-compaction** — Background cleanup of acknowledged segments
- ✅ **Durability guarantees** — Configurable fsync policies
- ✅ **Introspection API** — Queue stats (segment count, disk usage, offsets)
- ✅ **Graceful shutdown** — Ensures in-flight operations are persisted
- ✅ **Thread-safe** — Safe concurrent access from multiple goroutines
- ✅ **Platform-agnostic** — Works on Linux, macOS, Windows

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

	"github.com/vnykmshr/ledgerq"
)

func main() {
	// Open a queue (creates directory if needed)
	q, err := ledgerq.Open("./myqueue")
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue a message
	if err := q.Enqueue([]byte("hello, world")); err != nil {
		log.Fatal(err)
	}

	// Dequeue a message
	msg, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Message: %s\n", msg.Data)

	// Acknowledge the message
	if err := q.Ack(msg.ID); err != nil {
		log.Fatal(err)
	}
}
```

### Batch Operations

```go
// Enqueue multiple messages efficiently
batch := [][]byte{
	[]byte("message 1"),
	[]byte("message 2"),
	[]byte("message 3"),
}
if err := q.EnqueueBatch(batch); err != nil {
	log.Fatal(err)
}

// Dequeue up to 10 messages
messages, err := q.DequeueBatch(10)
if err != nil {
	log.Fatal(err)
}
```

### Replay

```go
// Replay from a specific offset
if err := q.ReplayFrom(ledgerq.FromOffset(100)); err != nil {
	log.Fatal(err)
}

// Replay from a timestamp
if err := q.ReplayFrom(ledgerq.FromTimestamp(time.Now().Add(-1 * time.Hour))); err != nil {
	log.Fatal(err)
}
```

### Statistics

```go
stats := q.Stats()
fmt.Printf("Total messages: %d\n", stats.TotalMessages)
fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
fmt.Printf("Disk usage: %d bytes\n", stats.DiskUsageBytes)
```

## Use Cases

- **Offline task queue** — Buffer background jobs or webhooks locally
- **Telemetry/event buffer** — Persist analytics or traces offline
- **Local event sourcing** — Log domain events or replay system state
- **Edge/IoT queue** — Message persistence in constrained/intermittent networks
- **Job replay testing** — Reproduce job batches deterministically

## Documentation

- [Architecture](docs/architecture.md) — Internal design and file format
- [API Reference](https://pkg.go.dev/github.com/vnykmshr/ledgerq) — Complete API documentation
- [Performance Guide](docs/performance.md) — Tuning and benchmarks

## Development

### Prerequisites

- Go 1.21 or later
- golangci-lint (for linting)

### Building

```bash
make build
```

### Testing

```bash
# Run tests
make test

# Run tests with coverage
make coverage

# Run benchmarks
make bench
```

### Linting

```bash
make lint
```

## Project Status

LedgerQ is currently in **active development** (pre-v1.0). The API may change as we refine the design based on feedback and real-world usage.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

LedgerQ is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgments

Inspired by:
- Python's [persist-queue](https://github.com/peter-wangxu/persist-queue)
- [bbolt](https://github.com/etcd-io/bbolt) for embedded storage patterns
- [badger](https://github.com/dgraph-io/badger) for LSM-tree design
