# Message Replay and Seeking Example

Navigate through message history with seek operations - time travel through your queue.

## What You'll Learn

- Seeking to specific message IDs
- Seeking by timestamp (time-based replay)
- Replaying message history
- Resetting to beginning
- Use cases for debugging and reprocessing

## Running the Example

```bash
go run main.go
```

## Sample Output

```
LedgerQ Replay Example
======================

✓ Queue opened at: /tmp/ledgerq-replay-example

Enqueuing messages with delays to capture different timestamps...
  [ID:1, Offset:64] Event 1: Application started
  [ID:2, Offset:122] Event 2: User logged in
  [ID:3, Offset:175] Event 3: User viewed dashboard
  [...]

--- Demo 1: Seek to Message ID 5 ---
Reading 3 messages from ID 5:
  [ID:5] Event 5: Data processed
  [ID:6] Event 6: Report generated
  [ID:7] Event 7: Email sent

--- Demo 2: Seek Back to Beginning ---
Reading first 3 messages:
  [ID:1] Event 1: Application started
  [ID:2] Event 2: User logged in
  [ID:3] Event 3: User viewed dashboard

--- Demo 3: Seek by Timestamp ---
Seeking to timestamp: 1761046396960639000
Reading 3 messages from that timestamp:
  [ID:7] Event 7: Email sent
  [ID:8] Event 8: User logged out
  [ID:9] Event 9: Cache cleared

--- Demo 4: Replay Entire Log ---
Replaying all events:
  [17:18:16.756] [ID:1] Event 1: Application started
  [17:18:16.756] [ID:2] Event 2: User logged in
  [17:18:16.858] [ID:4] Event 4: User clicked button
  [...]
  ✓ Replayed 10 events successfully!
```

## Key Concepts

### 1. Seek to Message ID

```go
// Jump to specific message ID
if err := q.SeekToMessageID(5); err != nil {
    log.Fatal(err)
}

// Next dequeue returns message ID 5
msg, _ := q.Dequeue()
fmt.Printf("ID: %d\n", msg.ID)  // 5
```

### 2. Seek by Timestamp

```go
// Find message at or after this timestamp
targetTime := time.Now().Add(-1 * time.Hour)
if err := q.SeekToTime(targetTime.UnixNano()); err != nil {
    log.Fatal(err)
}

// Next dequeue returns first message >= targetTime
msg, _ := q.Dequeue()
```

### 3. Seek to Beginning

```go
// Reset to start of queue
if err := q.SeekToMessageID(1); err != nil {
    log.Fatal(err)
}

// Replay from beginning
for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    reprocess(msg)
}
```

### 4. Get Current Position

```go
stats := q.Stats()
fmt.Printf("Read message ID: %d\n", stats.ReadMessageID)
fmt.Printf("Next message ID: %d\n", stats.NextMessageID)
```

## How It Works

```
Message Log on Disk:
[ID:1][ID:2][ID:3][ID:4][ID:5][ID:6][ID:7]...
  ↑                    ↑
  Start            Current read position

SeekToMessageID(3)
         ↓
[ID:1][ID:2][ID:3][ID:4][ID:5][ID:6][ID:7]...
              ↑
        New read position

Next Dequeue() returns ID:3
```

**Implementation**:
- Uses sparse index for fast seeking (~4KB granularity)
- Binary search to find segment containing target
- Sequential scan within segment to exact position
- O(log N) seek time for N segments

## Use Cases

### 1. Debugging Production Issues

```go
// Find when error started
errorTime := time.Date(2025, 10, 21, 14, 30, 0, 0, time.UTC)
q.SeekToTime(errorTime.UnixNano())

// Replay messages to reproduce issue
for i := 0; i < 100; i++ {
    msg, _ := q.Dequeue()
    if causesError(msg.Payload) {
        fmt.Printf("Found culprit: %s\n", msg.Payload)
        break
    }
}
```

### 2. Reprocess After Bug Fix

```go
// Reset to message where bug was introduced
q.SeekToMessageID(buggyMessageID)

// Reprocess with fix
for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    processWithFix(msg.Payload)
}
```

### 3. Event Sourcing Replay

```go
// Rebuild read model from events
q.SeekToMessageID(1)  // Start from beginning

state := NewReadModel()
for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    state.Apply(parseEvent(msg.Payload))
}
```

### 4. Time-Based Auditing

```go
// Audit all events in specific time window
startTime := time.Date(2025, 10, 21, 9, 0, 0, 0, time.UTC)
endTime := time.Date(2025, 10, 21, 17, 0, 0, 0, time.UTC)

q.SeekToTime(startTime.UnixNano())
for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    if msg.Timestamp > endTime.UnixNano() {
        break  // Past end of window
    }
    auditEvent(msg)
}
```

### 5. Partial Reprocessing

```go
// Process specific range
startID := 1000
endID := 2000

q.SeekToMessageID(startID)
for i := startID; i < endID; i++ {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    reprocess(msg)
}
```

### 6. Duplicate Consumer Recovery

```go
// Consumer crashed at message 567
// Restart from last known position
lastProcessedID := loadCheckpoint()  // e.g., 567

q.SeekToMessageID(lastProcessedID + 1)  // Resume at 568

for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    process(msg)
    saveCheckpoint(msg.ID)
}
```

## Performance

- **SeekToMessageID**: ~500 microseconds (binary search + scan)
- **SeekToTime**: ~1 millisecond (timestamp index lookup)
- **Sparse index**: ~1 entry per 4KB, very small memory footprint
- **No disk writes**: Seeking only updates in-memory read position

For 1 million messages:
- Index size: ~250 KB
- Seek time: ~500 μs

## Best Practices

**✅ DO:**
- Use SeekToMessageID for exact replay from known position
- Use SeekToTime for time-based auditing
- Save read position (checkpointing) for recovery
- Reset to beginning for full replays
- Test replay logic with small datasets first

**❌ DON'T:**
- Seek in tight loops (expensive operation)
- Seek while multiple consumers are active (race conditions)
- Use for random access (use database instead)
- Forget to handle `ErrNoMessages` after seeking
- Seek to future message IDs (returns error)

## Error Handling

```go
// Seek to non-existent message ID
if err := q.SeekToMessageID(99999); err != nil {
    if err == ledgerq.ErrInvalidMessageID {
        fmt.Println("Message ID does not exist")
    }
}

// Seek to future timestamp
futureTime := time.Now().Add(1 * time.Hour)
if err := q.SeekToTime(futureTime.UnixNano()); err != nil {
    // No messages at this timestamp yet
}
```

## Monitoring Replay Progress

```go
// Track replay progress
startID := 1
endID := q.Stats().NextMessageID - 1
totalMessages := endID - startID + 1

q.SeekToMessageID(startID)
processed := 0

for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    process(msg)
    processed++

    // Report progress every 100 messages
    if processed%100 == 0 {
        progress := float64(processed) / float64(totalMessages) * 100
        fmt.Printf("Replay progress: %.1f%% (%d/%d)\n",
            progress, processed, totalMessages)
    }
}
```

## Combining with Other Features

### Replay + Headers

```go
// Replay messages of specific type
q.SeekToMessageID(1)
for {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    if msg.Headers["event-type"] == "OrderCreated" {
        reprocessOrder(msg.Payload)
    }
}
```

### Replay + Checkpointing

```go
// Checkpoint every 10 messages
checkpointInterval := 10
q.SeekToMessageID(loadCheckpoint())

for i := 0; ; i++ {
    msg, err := q.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    process(msg)

    if i%checkpointInterval == 0 {
        saveCheckpoint(msg.ID)
    }
}
```

## Persistence

**Read position persists across restarts**:
- Saved to queue metadata automatically
- No explicit save needed
- Use `Close()` to ensure metadata synced to disk

**Checkpoint pattern for safety**:
```go
// Manual checkpointing for extra safety
checkpoint := q.Stats().ReadMessageID
// Save checkpoint to external storage
db.SaveCheckpoint("queue-consumer", checkpoint)

// On restart
lastCheckpoint := db.LoadCheckpoint("queue-consumer")
q.SeekToMessageID(lastCheckpoint)
```

## Troubleshooting

**Seek returning wrong messages?**
- Check message ID vs offset confusion (use ID, not offset)
- Verify timestamp in nanoseconds: `time.UnixNano()`

**Slow seek performance?**
- Seeking across many segments is slower
- Consider compaction to reduce segment count

**Can't seek to specific ID?**
- Message may have been compacted away
- Check `stats.NextMessageID` for valid range

## Next Steps

- **[simple](../simple/)** - Basic sequential processing
- **[streaming](../streaming/)** - Real-time event streaming (no replay)
- **[headers](../headers/)** - Filter during replay

---

**Difficulty**: 🟡 Intermediate | **Version**: v1.0.0+ | **Use Case**: Debugging, reprocessing, event sourcing
