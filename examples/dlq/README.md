# Dead Letter Queue (DLQ) Example (v1.2.0+)

Handle processing failures gracefully with automatic retry and DLQ.

## What You'll Learn

- Configuring DLQ with retry limits
- Using `Ack()` for successful processing
- Using `Nack()` to report failures
- Automatic DLQ movement after max retries
- Inspecting failed messages
- Requeuing from DLQ back to main queue

## Running the Example

```bash
go run main.go
```

## Sample Output

```
LedgerQ Dead Letter Queue (DLQ) Example
========================================

✓ Queue opened with DLQ enabled
  Queue directory: /tmp/ledgerq-dlq-example
  DLQ directory:   /tmp/ledgerq-dlq-example-dlq
  Max retries:     3

1. Enqueuing Messages
   ------------------
  ✓ Enqueued: Task: Process payment #1001 (normal)
  ✓ Enqueued: Task: Send email notification (will fail)
  ✓ Enqueued: Task: Update inventory (normal)
  ✓ Enqueued: Task: Generate report (will fail)

2. Processing Messages
   -------------------
  Processing [ID:1]: Task: Process payment #1001
    → ✓ Success (Acked)
  Processing [ID:2]: Task: Send email notification
    → Processing failed, will retry...
    → Attempt 1 failed, will retry...
    → Attempt 2 failed, will retry...
    → Attempt 3 failed, moved to DLQ!

3. Inspecting Dead Letter Queue
   -----------------------------
  DLQ has 2 message(s)

  Failed messages in DLQ:

  Message 1:
    DLQ ID:           1
    Payload:          Task: Send email notification
    Original Msg ID:  2
    Retry Count:      3
    Failure Reason:   Simulated failure (attempt 3/3)
    Last Failure:     2025-10-21T17:17:43.217+05:45

4. Requeuing from DLQ
   ------------------
  Requeuing message: Task: Send email notification
  ✓ Message requeued to main queue
  Processing requeued message [ID:5]: Task: Send email notification
  ✓ Requeued message processed successfully!
```

## Key Concepts

### 1. Enable DLQ

```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DLQPath = "/path/to/dlq"  // Must be OUTSIDE queue directory
opts.MaxRetries = 3             // Fail after 3 nacks
q, _ := ledgerq.Open("/path/to/queue", opts)
```

**IMPORTANT**: DLQ path cannot be a subdirectory of the queue path.

### 2. Acknowledge Successful Processing

```go
msg, _ := q.Dequeue()
// Process the message...
if err := processMessage(msg.Payload); err == nil {
    q.Ack(msg.ID)  // Mark as successfully processed
}
```

### 3. Report Processing Failures

```go
msg, _ := q.Dequeue()
if err := processMessage(msg.Payload); err != nil {
    q.Nack(msg.ID, err.Error())  // Will retry or move to DLQ
}
```

**What happens on Nack**:
- **Retry count < MaxRetries**: Message requeued for retry
- **Retry count = MaxRetries**: Message moved to DLQ

### 4. Access DLQ

```go
dlq := q.GetDLQ()
if dlq == nil {
    // DLQ not configured
}

// DLQ is a regular LedgerQ instance
dlqMsg, _ := dlq.Dequeue()
```

### 5. Inspect DLQ Message Metadata

```go
dlqMsg, _ := dlq.Dequeue()

// Automatic headers added by LedgerQ:
originalID := dlqMsg.Headers["dlq.original_msg_id"]  // Main queue message ID
retryCount := dlqMsg.Headers["dlq.retry_count"]      // "3"
reason := dlqMsg.Headers["dlq.failure_reason"]       // Last error message
timestamp := dlqMsg.Headers["dlq.last_failure"]      // ISO 8601 timestamp
```

### 6. Requeue from DLQ

```go
// Option 1: Use RequeueFromDLQ (preserves metadata)
q.RequeueFromDLQ(dlqMsg.ID)

// Option 2: Manual enqueue (starts fresh)
q.Enqueue(dlqMsg.Payload)
```

## How It Works

```
Dequeue() → Process() → Success?
                          ↓
                        YES → Ack()
                          ↓
                     Message completed

                          NO → Nack(reason)
                          ↓
                 Retry count++
                          ↓
             Retry < MaxRetries?
                    ↓           ↓
                  YES          NO
                    ↓           ↓
            Requeue for    Move to DLQ
              retry       (preserve metadata)
```

**Retry tracking**: Stored in queue metadata, survives restarts.

## Use Cases

### 1. External API Failures

```go
func processOrder(q *ledgerq.LedgerQ) {
    msg, _ := q.Dequeue()

    // Call payment gateway
    if err := chargePayment(msg.Payload); err != nil {
        q.Nack(msg.ID, fmt.Sprintf("Payment failed: %v", err))
        return  // Will retry or go to DLQ
    }

    q.Ack(msg.ID)  // Success
}
```

### 2. Database Deadlocks

```go
msg, _ := q.Dequeue()
if err := db.Transaction(func(tx *sql.Tx) error {
    // Complex multi-table update
    return updateInventory(tx, msg.Payload)
}); err != nil {
    if isDeadlock(err) {
        q.Nack(msg.ID, "Deadlock detected")  // Retry
    } else {
        q.Nack(msg.ID, fmt.Sprintf("DB error: %v", err))  // Will eventually DLQ
    }
    return
}
q.Ack(msg.ID)
```

### 3. DLQ Monitoring and Alerting

```go
// Check DLQ periodically
dlq := q.GetDLQ()
stats := dlq.Stats()

if stats.PendingMessages > 100 {
    alert.Send("DLQ has %d messages - investigate!", stats.PendingMessages)
}
```

### 4. Manual DLQ Processing

```go
// Process DLQ messages manually (e.g., after fixing bug)
dlq := q.GetDLQ()
for {
    msg, err := dlq.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }

    // Reprocess with updated logic
    if err := reprocessWithFix(msg.Payload); err == nil {
        dlq.Ack(msg.ID)  // Remove from DLQ
        log.Printf("Fixed message %s", msg.Headers["dlq.original_msg_id"])
    } else {
        // Still failing - leave in DLQ for investigation
        log.Printf("Still failing: %v", err)
    }
}
```

## Performance

- **Ack/Nack overhead**: ~2 microseconds (in-memory metadata update)
- **DLQ move operation**: ~500 microseconds (write to DLQ queue)
- **Retry requeue**: ~200 nanoseconds (in-memory re-insert)

No disk I/O for Ack/Nack - metadata synced periodically.

## Best Practices

**✅ DO:**
- Always use Ack() after successful processing
- Include detailed error messages in Nack()
- Monitor DLQ size (alert if growing)
- Set MaxRetries based on failure type (3-5 typical)
- Log DLQ movements for debugging

**❌ DON'T:**
- Use DLQ as permanent storage (process or delete eventually)
- Set MaxRetries too high (delays DLQ movement)
- Ignore DLQ messages (defeats purpose)
- Nack() for validation errors (fail fast instead)
- Requeue from DLQ without fixing root cause

## Troubleshooting

**Messages not moving to DLQ?**
- Check `MaxRetries` is set (default 0 = disabled)
- Verify you're calling `Nack()`, not just dequeuing again
- Ensure DLQ path is configured

**DLQ directory error?**
- DLQ path must be OUTSIDE queue directory
- Use sibling directories: `/tmp/queue` and `/tmp/queue-dlq`

**Lost retry counts after restart?**
- Retry metadata persists in queue metadata
- Check queue closed cleanly (deferred `Close()`)

## Configuration

```go
opts := ledgerq.DefaultOptions("/queue")

// DLQ configuration
opts.DLQPath = "/dlq"          // Required to enable DLQ
opts.MaxRetries = 3            // Default: 0 (DLQ disabled)

// Related options
opts.AutoSync = true           // Sync metadata immediately
opts.SyncInterval = 5 * time.Second  // Default metadata sync interval
```

## Monitoring

```go
// Main queue stats
mainStats := q.Stats()
fmt.Printf("Pending: %d\n", mainStats.PendingMessages)

// DLQ stats
dlq := q.GetDLQ()
dlqStats := dlq.Stats()
fmt.Printf("DLQ size: %d\n", dlqStats.PendingMessages)

// Calculate failure rate
failureRate := float64(dlqStats.TotalMessages) / float64(mainStats.TotalMessages) * 100
fmt.Printf("Failure rate: %.2f%%\n", failureRate)
```

## Next Steps

- **[priority](../priority/)** - Prioritize urgent failed messages
- **[deduplication](../deduplication/)** - Prevent duplicate DLQ requeues
- **[headers](../headers/)** - Add custom metadata to track failures

---

**Difficulty**: 🟡 Intermediate | **Version**: v1.2.0+ | **Use Case**: Reliable message processing
