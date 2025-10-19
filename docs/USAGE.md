# LedgerQ Usage Guide

Complete reference for using LedgerQ.

## Table of Contents

- [Installation](#installation)
- [Basic Operations](#basic-operations)
- [Configuration](#configuration)
- [Advanced Features](#advanced-features)
- [CLI Tool](#cli-tool)
- [Best Practices](#best-practices)
  - [Performance](#performance)
  - [Reliability](#reliability)
  - [Development](#development)
  - [Security](#security)
- [Troubleshooting](#troubleshooting)

## Installation

```bash
go get github.com/vnykmshr/ledgerq/pkg/ledgerq@latest
```

Import in your code:

```go
import "github.com/vnykmshr/ledgerq/pkg/ledgerq"
```

## Basic Operations

### Opening a Queue

**Default configuration**:
```go
q, err := ledgerq.Open("/path/to/queue", nil)
if err != nil {
    log.Fatal(err)
}
defer q.Close()
```

**Custom configuration**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.AutoSync = true
opts.SyncInterval = 1 * time.Second
opts.MaxSegmentSize = 100 * 1024 * 1024 // 100MB

q, err := ledgerq.Open("/path/to/queue", opts)
```

### Enqueuing Messages

**Single message**:
```go
offset, err := q.Enqueue([]byte("Hello, World!"))
if err != nil {
    return err
}
fmt.Printf("Enqueued at offset: %d\n", offset)
```

**Batch enqueue** (recommended for high throughput):
```go
payloads := [][]byte{
    []byte("message1"),
    []byte("message2"),
    []byte("message3"),
}
offsets, err := q.EnqueueBatch(payloads)
if err != nil {
    return err
}
```

Batching provides 10-100x performance improvement with a single fsync.

### Dequeuing Messages

**Single message**:
```go
msg, err := q.Dequeue()
if err != nil {
    // Queue might be empty or closed
    return err
}
fmt.Printf("Message ID: %d, Payload: %s\n", msg.ID, msg.Payload)
```

**Batch dequeue**:
```go
messages, err := q.DequeueBatch(10) // Up to 10 messages
if err != nil {
    return err
}

for _, msg := range messages {
    fmt.Printf("Processing: %s\n", msg.Payload)
}
```

### Closing the Queue

```go
if err := q.Close(); err != nil {
    log.Printf("Error closing queue: %v", err)
}
```

## Configuration

### Options Reference

```go
type Options struct {
    // Sync behavior
    AutoSync       bool          // fsync after every write (default: false)
    SyncInterval   time.Duration // Periodic sync interval (default: 1s)

    // Compaction
    CompactionInterval time.Duration // Auto-compaction interval (0 = disabled)

    // Segment rotation
    MaxSegmentSize     uint64         // Max bytes per segment (default: 1GB)
    MaxSegmentMessages uint64         // Max messages per segment (0 = unlimited)
    RotationPolicy     RotationPolicy // When to rotate segments

    // Retention
    RetentionPolicy *RetentionPolicy  // Segment cleanup policy (nil = no cleanup)

    // Observability
    Logger           Logger            // Structured logging (nil = no logging)
    MetricsCollector MetricsCollector // Metrics collection (nil = no metrics)
}
```

### Rotation Policies

**By size** (default):
```go
opts.RotationPolicy = ledgerq.RotateBySize
opts.MaxSegmentSize = 100 * 1024 * 1024 // 100MB
```

**By message count**:
```go
opts.RotationPolicy = ledgerq.RotateByCount
opts.MaxSegmentMessages = 1000000 // 1M messages
```

**By both** (whichever comes first):
```go
opts.RotationPolicy = ledgerq.RotateByBoth
opts.MaxSegmentSize = 100 * 1024 * 1024
opts.MaxSegmentMessages = 1000000
```

### Retention Policies

Configure automatic cleanup of old segments:

```go
opts.RetentionPolicy = &ledgerq.RetentionPolicy{
    MaxAge:      7 * 24 * time.Hour,          // Delete segments older than 7 days
    MaxSize:     10 * 1024 * 1024 * 1024,     // Keep total size under 10GB
    MaxSegments: 100,                          // Keep max 100 segments
    MinSegments: 1,                            // Always keep at least 1
}
```

## Advanced Features

### Message TTL (Time-To-Live)

Enqueue messages that expire automatically:

```go
// Expire after 5 seconds
offset, err := q.EnqueueWithTTL([]byte("temporary"), 5*time.Second)
```

**How it works**:
- Expired messages are skipped during dequeue
- No background cleanup (lazy expiration)
- TTL persists across restarts
- Zero overhead for non-TTL messages

**Use cases**:
- Temporary task queues
- Time-sensitive notifications
- Cache invalidation messages
- Session management

### Message Headers

Attach metadata to messages:

```go
headers := map[string]string{
    "content-type":   "application/json",
    "correlation-id": "req-12345",
    "priority":       "high",
}
offset, err := q.EnqueueWithHeaders(payload, headers)
```

**Combine TTL and headers**:
```go
offset, err := q.EnqueueWithOptions(payload, 10*time.Second, headers)
```

**Use cases**:
- Message routing and classification
- Distributed tracing (trace-id, span-id)
- Event sourcing metadata
- Content-type indication

### Streaming API

Process messages in real-time with context support:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

handler := func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    // Return error to stop streaming
    return nil
}

err := q.Stream(ctx, handler)
if err != nil && err != context.Canceled {
    log.Printf("Stream error: %v", err)
}
```

**Benefits**:
- Push-based (no polling loop needed)
- Graceful shutdown via context
- Cleaner error handling

### Replay Operations

Reprocess messages from a specific point:

**Seek by message ID**:
```go
err := q.SeekToMessageID(100)
if err != nil {
    return err
}
// Next Dequeue() starts from message ID 100
```

**Seek by timestamp**:
```go
oneHourAgo := time.Now().Add(-1 * time.Hour).UnixNano()
err := q.SeekToTimestamp(oneHourAgo)
if err != nil {
    return err
}
// Next Dequeue() starts from first message after timestamp
```

**Use cases**:
- Replay events for testing
- Reprocess failed batches
- Audit log analysis
- Time-based recovery

### Queue Statistics

Monitor queue health:

```go
stats := q.Stats()
fmt.Printf("Total messages:   %d\n", stats.TotalMessages)
fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
fmt.Printf("Next ID:          %d\n", stats.NextMessageID)
fmt.Printf("Read position:    %d\n", stats.ReadMessageID)
fmt.Printf("Segments:         %d\n", stats.SegmentCount)
```

### Metrics Collection

Collect performance metrics:

```go
// Create collector
collector := ledgerq.NewMetricsCollector("my_queue")

opts := ledgerq.DefaultOptions("/path/to/queue")
opts.MetricsCollector = collector

q, _ := ledgerq.Open("/path/to/queue", opts)

// Perform operations...
q.Enqueue([]byte("test"))
q.Dequeue()

// Get snapshot
snapshot := ledgerq.GetMetricsSnapshot(collector)
fmt.Printf("Enqueue Total:    %d\n", snapshot.EnqueueTotal)
fmt.Printf("Dequeue Total:    %d\n", snapshot.DequeueTotal)
fmt.Printf("Enqueue P95:      %v\n", snapshot.EnqueueDurationP95)
fmt.Printf("Pending Messages: %d\n", snapshot.PendingMessages)
```

**Available metrics**:
- Operation counters (enqueue, dequeue, batch, errors)
- Payload bytes (enqueued, dequeued)
- Duration percentiles (P50, P95, P99)
- Queue state (pending, segments, IDs)
- Compaction metrics

### Logging

**Implement custom logger**:

LedgerQ does not export a default logger implementation. To enable logging, implement the `Logger` interface:
```go
// Logger interface that must be implemented
type Logger interface {
    Debug(msg string, fields ...LogField)
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
}

// Example implementation using standard log package
type SimpleLogger struct{}

func (l *SimpleLogger) Debug(msg string, fields ...ledgerq.LogField) {
    log.Printf("[DEBUG] %s %v", msg, fields)
}
func (l *SimpleLogger) Info(msg string, fields ...ledgerq.LogField) {
    log.Printf("[INFO] %s %v", msg, fields)
}
func (l *SimpleLogger) Warn(msg string, fields ...ledgerq.LogField) {
    log.Printf("[WARN] %s %v", msg, fields)
}
func (l *SimpleLogger) Error(msg string, fields ...ledgerq.LogField) {
    log.Printf("[ERROR] %s %v", msg, fields)
}

// Use custom logger
opts.Logger = &SimpleLogger{}
```

### Manual Compaction

Trigger compaction on demand:

```go
result, err := q.Compact()
if err != nil {
    return err
}
fmt.Printf("Removed %d segments, freed %d bytes\n",
    result.SegmentsRemoved, result.BytesFreed)
```

**Automatic compaction**:
```go
opts.CompactionInterval = 5 * time.Minute
// Compaction runs in background
```

## CLI Tool

### Installation

```bash
go install github.com/vnykmshr/ledgerq/cmd/ledgerq@latest
```

### Commands

**Show statistics**:
```bash
ledgerq stats /path/to/queue
```

**Inspect queue (JSON)**:
```bash
ledgerq inspect /path/to/queue
```

**Compact manually**:
```bash
ledgerq compact /path/to/queue
```

**Peek messages** (without consuming):
```bash
ledgerq peek /path/to/queue 5
```

**Show version**:
```bash
ledgerq version
```

## Best Practices

### Performance

1. **Use batch operations** for high throughput
   - Single fsync for entire batch
   - 10-100x faster than individual operations

2. **Disable AutoSync** for better performance
   - Use periodic sync instead (SyncInterval)
   - Risk: potential data loss on crash
   - Mitigation: Use batch operations

3. **Right-size segments**
   - Too small: Frequent rotation overhead
   - Too large: Slower compaction, recovery
   - Recommended: 100MB - 1GB

4. **Monitor metrics**
   - Watch pending message count
   - Track dequeue latency percentiles
   - Alert on compaction failures

### Reliability

1. **Always close queues properly**
   - Use `defer q.Close()` pattern
   - Ensures data is flushed to disk

2. **Handle errors appropriately**
   - Queue might be closed
   - Disk might be full
   - Data might be corrupted

3. **Use AutoSync for critical data**
   - Guarantees durability at cost of performance
   - Or use batch operations with periodic sync

4. **Configure retention policies**
   - Prevent disk from filling up
   - Keep MinSegments > 0 for safety

### Development

1. **Use examples as templates**
   - See `examples/` directory
   - Cover common patterns

2. **Run tests before deploying**
   - `go test -race ./...`
   - Catches concurrency issues

3. **Enable logging during development**
   - Debug issues more easily
   - Disable in production for performance

### Security

**File Permissions**: Queue data files are created with 0644 permissions (world-readable). For sensitive data, restrict access using parent directory permissions:

```go
// Create queue in user-only directory
queueDir := "/var/app/queues/sensitive"
if err := os.MkdirAll(queueDir, 0700); err != nil {
    log.Fatal(err)
}
q, err := ledgerq.Open(queueDir, nil)
```

**Additional Security Considerations**:
- Validate and sanitize data before enqueueing
- Consider application-level encryption for sensitive payloads
- Avoid placing queue directories in web-accessible locations
- Use dedicated service accounts on multi-user systems
- Monitor queue statistics for unusual activity

For complete security policy and audit results, see:
- [SECURITY.md](../SECURITY.md) - Vulnerability reporting and security policy
- [SECURITY_AUDIT.md](SECURITY_AUDIT.md) - Detailed security audit findings

## Troubleshooting

### Queue won't open

**Error**: "failed to create directory"
- **Cause**: Permission issues
- **Fix**: Ensure directory is writable

**Error**: "invalid segment sequence"
- **Cause**: Corrupted or missing segment files
- **Fix**: Restore from backup or delete corrupted segments

### Messages not persisting

**Cause**: AutoSync disabled, process crashed before sync
- **Fix**: Enable AutoSync or use shorter SyncInterval

### Performance degradation

**Cause**: Too many small segments
- **Fix**: Increase MaxSegmentSize or enable compaction

**Cause**: Disk full
- **Fix**: Configure retention policy or free up space

### High memory usage

**Cause**: Large batch operations
- **Fix**: Reduce batch size

**Cause**: Metrics collecting too much history
- **Fix**: Reset metrics periodically or disable

### Compaction not running

**Cause**: CompactionInterval = 0
- **Fix**: Set non-zero interval or use manual compaction

**Cause**: No retention policy configured
- **Fix**: Configure RetentionPolicy

## See Also

- [Architecture](ARCHITECTURE.md) - Internal design details
- [Examples](../examples/) - Runnable code examples
- [API Documentation](https://pkg.go.dev/github.com/vnykmshr/ledgerq/pkg/ledgerq) - GoDoc reference
