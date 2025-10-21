# Simple Example - Getting Started with LedgerQ

⭐ **Start here if you're new to LedgerQ!**

This example demonstrates the core operations: open, enqueue, dequeue, and stats.

## What You'll Learn

- Opening a queue with default configuration
- Enqueuing messages (writing to queue)
- Dequeuing messages (reading from queue)
- Checking queue statistics
- Proper cleanup with defer

## Running the Example

```bash
go run main.go
```

## Sample Output

```
LedgerQ Simple Example
======================

✓ Queue opened at: /tmp/ledgerq-simple-example

Enqueuing messages:
  1. Enqueued at offset 64: Hello, World!
  2. Enqueued at offset 107: This is LedgerQ
  3. Enqueued at offset 152: A persistent message queue
  4. Enqueued at offset 208: Written in Go

Queue Stats:
  Total messages:   4
  Pending messages: 4
  Segments:         1

Dequeuing messages:
  1. [ID:1] Hello, World!
  2. [ID:2] This is LedgerQ
  3. [ID:3] A persistent message queue
  4. [ID:4] Written in Go

Final Stats:
  Total messages:   4
  Pending messages: 0

✓ Example completed successfully!
```

## Key Concepts

### 1. Opening a Queue

```go
q, err := ledgerq.Open("/path/to/queue", nil)
if err != nil {
    log.Fatal(err)
}
defer q.Close()  // Always close when done
```

- First argument: directory path (created if doesn't exist)
- Second argument: `nil` uses default options
- Always close the queue with `defer` for proper cleanup

### 2. Enqueuing Messages

```go
offset, err := q.Enqueue([]byte("Hello, World!"))
```

- Takes `[]byte` payload (convert strings with `[]byte()`)
- Returns offset (unique ID for this message)
- Thread-safe - multiple goroutines can enqueue simultaneously

### 3. Dequeuing Messages

```go
msg, err := q.Dequeue()
fmt.Printf("[ID:%d] %s\n", msg.ID, msg.Payload)
```

- Returns next unread message (FIFO order)
- `msg.ID`: Unique message identifier
- `msg.Payload`: Your data as `[]byte`
- Read position persists across restarts

### 4. Queue Statistics

```go
stats := q.Stats()
fmt.Printf("Total: %d, Pending: %d\n",
    stats.TotalMessages, stats.PendingMessages)
```

- `TotalMessages`: All messages ever enqueued
- `PendingMessages`: Unread messages waiting
- `SegmentCount`: Number of storage files

## How It Works

```
┌─────────────┐
│  Enqueue()  │ → Write to disk → Append-only log
└─────────────┘                    ↓
                                [msg1][msg2][msg3][msg4]
                                   ↑
┌─────────────┐                   │
│  Dequeue()  │ ← Read from disk ←┘
└─────────────┘
```

1. Messages written sequentially to disk (append-only)
2. Each message gets unique monotonic ID
3. Read position tracks what's been consumed
4. Crash-safe: power loss won't lose data

## Next Steps

Try these examples next:
- **[producer-consumer](../producer-consumer/)** - Multiple goroutines
- **[ttl](../ttl/)** - Message expiration
- **[headers](../headers/)** - Message metadata

## Common Patterns

### Error Handling

```go
msg, err := q.Dequeue()
if err != nil {
    if err == ledgerq.ErrNoMessages {
        // Queue is empty - wait or retry
        time.Sleep(100 * time.Millisecond)
    } else {
        log.Fatal(err)
    }
}
```

### Batch Processing

```go
// Enqueue multiple messages at once (faster)
payloads := [][]byte{
    []byte("msg1"),
    []byte("msg2"),
    []byte("msg3"),
}
offsets, err := q.EnqueueBatch(payloads)

// Dequeue up to 10 messages
messages, err := q.DequeueBatch(10)
```

### Temporary Queue

```go
// Auto-cleanup on exit
queueDir := "/tmp/myqueue"
defer os.RemoveAll(queueDir)

q, _ := ledgerq.Open(queueDir, nil)
defer q.Close()
```

## Troubleshooting

**Queue won't open?**
- Check directory permissions (needs write access)
- Verify path is valid
- Ensure disk space available

**No messages when dequeuing?**
- Queue might be empty (`err == ledgerq.ErrNoMessages`)
- Check `stats.PendingMessages` count

**Want to reset?**
- Close queue and delete directory: `rm -rf /path/to/queue`
- Reopening will create fresh queue

## Configuration

This example uses defaults. For custom config:

```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.AutoSync = true              // Sync after each write (safer, slower)
opts.MaxSegmentSize = 100 * 1024 * 1024  // 100MB segments
q, err := ledgerq.Open("/path/to/queue", opts)
```

See [docs/USAGE.md](../../docs/USAGE.md#configuration) for all options.

---

**Difficulty**: 🟢 Beginner | **Time**: 2 minutes | **Lines of Code**: ~40
