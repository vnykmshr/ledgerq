# Priority Queue Example (v1.1.0+)

Process urgent messages first with three-level priority ordering.

## What You'll Learn

- Enabling priority queue mode
- Using High, Medium, Low priority levels
- Preventing low-priority starvation
- Combining priority with other features (compression, headers, TTL)
- Understanding the performance trade-offs

## Running the Example

```bash
go run main.go
```

## Sample Output

```
LedgerQ Priority Queue Example (v1.4.0)
========================================

Example 1: Basic Priority Ordering
-----------------------------------
Enqueuing messages in mixed order:
  [LOW   ] Low priority task 1
  [HIGH  ] High priority alert!
  [MEDIUM] Medium priority update
  [LOW   ] Low priority task 2
  [HIGH  ] High priority critical!

Dequeuing messages (priority order: High → Medium → Low):
  [HIGH  ] High priority alert!
  [HIGH  ] High priority critical!
  [MEDIUM] Medium priority update
  [LOW   ] Low priority task 1
  [LOW   ] Low priority task 2

Example 2: Starvation Prevention
----------------------------------
Enqueuing 10 high-priority messages...
Enqueuing 5 medium-priority messages...
Enqueuing 3 low-priority messages...

Processing first 18 messages:
  1. [HIGH  ] High priority message 1
  2. [HIGH  ] High priority message 2
  [...]
  10. [HIGH  ] High priority message 10
  11. [MEDIUM] Medium priority message 1  ← Medium processed before all High exhausted
  [...]

✓ Starvation prevention ensures medium/low messages eventually process
```

## Key Concepts

### 1. Enable Priority Queue

```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.EnablePriorities = true
q, _ := ledgerq.Open("/path/to/queue", opts)
```

**IMPORTANT**: Must be set at queue creation. Cannot be toggled later.

### 2. Enqueue with Priority

```go
// High priority (urgent tasks, alerts)
q.EnqueueWithPriority(payload, ledgerq.PriorityHigh)

// Medium priority (normal operations)
q.EnqueueWithPriority(payload, ledgerq.PriorityMedium)

// Low priority (background tasks, cleanup)
q.EnqueueWithPriority(payload, ledgerq.PriorityLow)
```

### 3. Dequeue (Priority Order)

```go
msg, err := q.Dequeue()
// Returns highest priority message available
// Order: High → Medium → Low
```

**Transparent**: Consumer code unchanged - `Dequeue()` automatically returns highest priority.

### 4. Combine with Other Features

```go
// Priority + Compression + Headers + TTL
messages := []ledgerq.BatchEnqueueOptions{
    {
        Payload:     largePayload,
        Priority:    ledgerq.PriorityHigh,
        Compression: ledgerq.CompressionGzip,
        Headers:     map[string]string{"type": "alert"},
        TTL:         1 * time.Hour,
    },
}
q.EnqueueBatchWithOptions(messages)
```

## How It Works

```
Enqueue with Priority
         ↓
In-Memory Index (3 queues)
         ↓
    ┌─────────────┐
    │ High Queue  │ → [msg1, msg5, msg8]
    ├─────────────┤
    │ Medium Queue│ → [msg2, msg6]
    ├─────────────┤
    │ Low Queue   │ → [msg3, msg4, msg7]
    └─────────────┘
         ↓
Dequeue() picks from High first
If High empty → Medium
If Medium empty → Low
         ↓
Starvation prevention: Round-robin if High > 10 consecutive
```

**In-Memory State**: Priority ordering maintained in RAM, rebuilt on restart by scanning disk.

## Use Cases

### 1. Alert Processing System

```go
func processEvent(event Event) {
    priority := ledgerq.PriorityMedium
    if event.Severity == "critical" {
        priority = ledgerq.PriorityHigh
    } else if event.Type == "audit_log" {
        priority = ledgerq.PriorityLow
    }

    q.EnqueueWithPriority(event.Data, priority)
}
```

### 2. Task Queue with SLA

```go
// API requests: 100ms SLA (High)
q.EnqueueWithPriority(apiRequest, ledgerq.PriorityHigh)

// Background sync: 5min SLA (Medium)
q.EnqueueWithPriority(syncTask, ledgerq.PriorityMedium)

// Cleanup jobs: best-effort (Low)
q.EnqueueWithPriority(cleanupTask, ledgerq.PriorityLow)
```

### 3. Multi-Tenant Processing

```go
// Premium customers
if customer.Tier == "premium" {
    q.EnqueueWithPriority(job, ledgerq.PriorityHigh)
} else {
    q.EnqueueWithPriority(job, ledgerq.PriorityLow)
}
```

### 4. Dead Letter Queue Reprocessing

```go
// DLQ retries get lower priority than new messages
dlqMsg, _ := dlq.Dequeue()
mainQueue.EnqueueWithPriority(dlqMsg.Payload, ledgerq.PriorityLow)
```

## Performance

- **Enqueue overhead**: ~200 ns/op (in-memory index insert)
- **Dequeue overhead**: ~150 ns/op (in-memory index lookup)
- **Memory usage**: ~40 bytes per message (offset + priority metadata)
- **Rebuild on restart**: ~1 million messages/sec (single-pass disk scan)

For 100K messages: ~4 MB in-memory index, ~100ms rebuild time.

## Starvation Prevention

```go
// Configuration (internal, automatic)
const starvationThreshold = 10
```

**How it works**:
1. Dequeue typically returns highest priority
2. If 10+ consecutive High messages dequeued → force check Medium
3. If 10+ consecutive Medium → force check Low
4. Prevents indefinite blocking of lower priorities

**Example**:
```
High queue: [H1, H2, H3, ..., H20]
Medium queue: [M1, M2]
Low queue: [L1]

Dequeue order: H1, H2, ..., H10, M1, H11, H12, ..., H20, M2, L1
                               ↑ Medium processed after 10 High
```

## Best Practices

**✅ DO:**
- Use 3 levels meaningfully (don't overuse High)
- Combine with TTL for time-sensitive high-priority messages
- Monitor queue depths per priority level
- Use for latency-sensitive systems

**❌ DON'T:**
- Enable priority if all messages have same urgency (overhead for no benefit)
- Assume strict ordering within same priority (FIFO within priority level)
- Rely on priority for security (use separate queues for isolation)
- Change `EnablePriority` on existing queue (requires recreation)

## Monitoring

```go
stats := q.Stats()
fmt.Printf("Total messages: %d\n", stats.TotalMessages)
fmt.Printf("Pending: %d\n", stats.PendingMessages)

// Check priority distribution (requires custom tracking)
```

**Tip**: Implement custom metrics to track enqueue/dequeue counts per priority level.

## Troubleshooting

**Low-priority messages never processed?**
- Check if High queue constantly has 10+ messages
- Starvation prevention should kick in - verify with logging
- Consider increasing consumer throughput

**Priority not working?**
- Verify `EnablePriorities = true` was set at queue creation
- Check you're using `EnqueueWithPriority()`, not `Enqueue()`
- Priority only affects dequeue order, not storage order

**High memory usage?**
- Each message adds ~40 bytes to in-memory index
- For 1M messages: ~40 MB overhead
- Consider segmentation or multiple queues if problematic

## Migration

**Enabling priority on existing queue:**

```go
// ❌ WRONG: Cannot toggle on existing queue
opts.EnablePriorities = true
q.Open("/existing/queue", opts)  // Will fail or ignore setting

// ✅ RIGHT: Create new priority queue and migrate
oldQ, _ := ledgerq.Open("/old/queue", nil)
newOpts := ledgerq.DefaultOptions("/new/queue")
newOpts.EnablePriorities = true
newQ, _ := ledgerq.Open("/new/queue", newOpts)

// Migrate messages with default Medium priority
for {
    msg, err := oldQ.Dequeue()
    if err == ledgerq.ErrNoMessages {
        break
    }
    newQ.EnqueueWithPriority(msg.Payload, ledgerq.PriorityMedium)
}
```

## Architecture Notes

**In-Memory Index**:
- Three FIFO queues (slices) holding message offsets
- Rebuilt on `Open()` by scanning all segments
- Lost on crash (rebuilt from disk on restart)
- Not persisted separately

**Disk Format**:
- Priority stored in entry flags (2 bits: 00=Low, 01=Medium, 10=High)
- Messages stored in disk order (not priority order)
- Index rebuilt by reading entry flags during startup

## Next Steps

- **[dlq](../dlq/)** - Combine with DLQ for prioritized error handling
- **[compression](../compression/)** - Add compression to large high-priority payloads
- **[deduplication](../deduplication/)** - Prevent duplicate high-priority processing

---

**Difficulty**: 🟡 Intermediate | **Version**: v1.1.0+ | **Use Case**: Latency-sensitive systems, SLA-based processing
