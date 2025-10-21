# Message Deduplication Example (v1.4.0+)

Prevent duplicate processing with idempotency keys. Perfect for exactly-once semantics.

## What You'll Learn

- Enabling deduplication with time windows
- Using business IDs as deduplication keys
- Custom per-message deduplication windows
- Idempotent message processing patterns
- Monitoring deduplication statistics

## Running the Example

```bash
go run main.go
```

## Sample Output

```
=== LedgerQ Message Deduplication Example ===

✓ Queue opened with deduplication enabled
  Default window: 5m0s
  Max entries: 100000

Example 1: Basic Deduplication
--------------------------------
First enqueue:  offset=64, duplicate=false ✓
Second enqueue: offset=64, duplicate=true ✓
  → Duplicate detected! Original at offset 64

Example 2: Custom Deduplication Window
---------------------------------------
Enqueued with 100ms window: offset=136, duplicate=false ✓
Immediate retry: offset=136, duplicate=true ✓
  Waiting for dedup window to expire...
After window expires: offset=207, duplicate=false ✓

Example 3: Multiple Unique Messages
------------------------------------
Message 1 [payment-001]: NEW (offset=278)
Message 2 [payment-002]: NEW (offset=343)
Message 3 [payment-003]: NEW (offset=408)
Message 4 [payment-001]: DUPLICATE (offset=278)
```

## Key Concepts

### 1. Enable Deduplication

```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DefaultDeduplicationWindow = 5 * time.Minute
opts.MaxDeduplicationEntries = 100000  // Max 100K tracked (~6.4 MB)
q, _ := ledgerq.Open("/path/to/queue", opts)
```

### 2. Enqueue with Dedup ID

```go
// Use business ID as dedup key
offset, isDup, err := q.EnqueueWithDedup(payload, "order-12345", 0)
if isDup {
    fmt.Printf("Duplicate! Original at offset %d\n", offset)
    // Message NOT enqueued, returns offset of original
} else {
    fmt.Printf("New message at offset %d\n", offset)
}
```

**Important**: Dedup ID should be a unique business identifier:
- Order ID: `"order-12345"`
- Request ID: `"req-abc-123"`
- Transaction ID: `"txn-555"`
- Event ID: `"webhook-789"`

### 3. Custom Time Windows

```go
// Short window for this message only
q.EnqueueWithDedup(payload, "payment-001", 1*time.Minute)

// Use queue default (pass 0)
q.EnqueueWithDedup(payload, "payment-002", 0)

// Long window for critical operations
q.EnqueueWithDedup(payload, "payment-003", 1*time.Hour)
```

## How It Works

```
EnqueueWithDedup("order-123")
         ↓
   SHA-256 hash
         ↓
   Check hash map
         ↓
    Exists? ────Yes──→ Return (original_offset, isDuplicate=true)
         │
        No
         ↓
  Enqueue normally
         ↓
  Track in map: {hash → {offset, expiresAt}}
         ↓
  Background cleanup (every 10s removes expired)
```

**Memory**: ~64 bytes per tracked entry. 100K entries ≈ 6.4 MB.

## Use Cases

### 1. Idempotent API Endpoints

```go
func handleOrder(orderID string, data []byte) error {
    offset, isDup, err := queue.EnqueueWithDedup(data, orderID, 10*time.Minute)
    if err != nil {
        return err
    }
    if isDup {
        log.Printf("Order %s already processed at offset %d", orderID, offset)
        return nil  // Safe to return success
    }
    // Process new order...
}
```

### 2. DLQ Requeue Safety

```go
// Prevent duplicate processing when requeuing from DLQ
dlqMsg, _ := dlq.Dequeue()
originalID := dlqMsg.Headers["dlq.original_msg_id"]

offset, isDup, _ := q.EnqueueWithDedup(
    dlqMsg.Payload,
    fmt.Sprintf("dlq-requeue-%s", originalID),
    1*time.Hour,
)
if !isDup {
    log.Printf("Requeued DLQ message: %s", originalID)
}
```

### 3. Webhook Deduplication

```go
// Same webhook event sent multiple times
func handleWebhook(eventID string, payload []byte) {
    _, isDup, _ := queue.EnqueueWithDedup(
        payload,
        fmt.Sprintf("webhook-%s", eventID),
        5*time.Minute,
    )
    if isDup {
        log.Printf("Webhook %s already received", eventID)
        return  // Ignore duplicate
    }
    // Process webhook...
}
```

### 4. At-Least-Once to Exactly-Once

```go
// Convert at-least-once delivery to exactly-once processing
for msg := range messageStream {
    _, isDup, _ := queue.EnqueueWithDedup(
        msg.Data,
        msg.MessageID,  // Use upstream message ID
        10*time.Minute,
    )
    if !isDup {
        // Process only once
        processMessage(msg.Data)
    }
    msg.Ack()  // Always ack upstream
}
```

## Performance

- **Check operation**: ~573 ns/op (O(1) hash lookup)
- **Track operation**: ~886 ns/op (O(1) hash insert)
- **Cleanup**: ~346 ns/op per entry
- **Memory**: 64 bytes per entry

Deduplication adds ~1 microsecond overhead per message.

## Monitoring

```go
stats := q.Stats()
fmt.Printf("Tracking %d unique messages\n", stats.DedupTrackedEntries)

// Log high water mark
if stats.DedupTrackedEntries > 80000 {
    log.Warn("Dedup table at 80% capacity")
}
```

## Persistence

Dedup state persists across queue restarts:
- Saved to `.dedup_state.json` in queue directory
- Atomic writes (temp file + rename)
- Expired entries skipped during save
- Loaded automatically on `Open()`

## Best Practices

**✅ DO:**
- Use stable business IDs (order ID, request ID)
- Set appropriate windows (5-30 minutes typical)
- Monitor `DedupTrackedEntries` stat
- Use shorter windows for high-volume systems

**❌ DON'T:**
- Use timestamps as dedup IDs (won't dedupe)
- Use payload hash (defeats purpose)
- Set windows > 1 hour (high memory usage)
- Rely on dedup for security (use for idempotency only)

## Troubleshooting

**Duplicates not detected?**
- Check dedup ID is identical for duplicates
- Verify window hasn't expired (`DefaultDeduplicationWindow`)
- Ensure deduplication is enabled in options

**High memory usage?**
- Reduce `DefaultDeduplicationWindow`
- Lower `MaxDeduplicationEntries`
- Check `stats.DedupTrackedEntries` - at limit?

**False positives (different messages marked duplicate)?**
- Dedup IDs must be globally unique
- Don't reuse IDs across message types

## Next Steps

- **[dlq](../dlq/)** - Combine with DLQ for robust error handling
- **[compression](../compression/)** - Reduce disk usage for large payloads
- **[priority](../priority/)** - Add priority ordering

---

**Difficulty**: 🟡 Intermediate | **Version**: v1.4.0+ | **Use Case**: Exactly-once semantics
