# Time-To-Live (TTL) Example

Automatically expire messages after a specified duration.

## What You'll Learn

- Setting TTL on individual messages
- Batch enqueue with TTL
- How expired messages are skipped
- Combining TTL with other features
- Use cases for temporary messages

## Running the Example

```bash
go run main.go
```

## Sample Output

```
=== LedgerQ TTL (Time-To-Live) Example ===

1. Enqueuing messages with TTL:
   Enqueued message with 2s TTL at offset 64
   Enqueued message with 5s TTL at offset 130
   Enqueued permanent message at offset 197

2. Immediate dequeue (before expiration):
   Dequeued: Short-lived message (2s TTL)
   Expires in: 2s
   Dequeued: Medium-lived message (5s TTL)
   Expires in: 5s
   Dequeued: Permanent message (no TTL)
   No expiration (permanent message)

3. Message expiration demonstration:
   Enqueued 5 messages (3 with TTL, 2 permanent)
   Waiting 2 seconds for TTL messages to expire...

   Dequeueing after expiration:
   Message 1: Permanent message (no TTL)
   Message 2: This message stays
   [Expired messages automatically skipped]
```

## Key Concepts

### 1. Enqueue with TTL

```go
import "time"

// Message expires in 10 seconds
ttl := 10 * time.Second
q.EnqueueWithTTL(payload, ttl)
```

### 2. Permanent Messages (No TTL)

```go
// These two are equivalent - no expiration
q.Enqueue(payload)                    // No TTL
q.EnqueueWithTTL(payload, 0)          // 0 = permanent
```

### 3. Batch Enqueue with TTL

```go
messages := []ledgerq.BatchEnqueueOptions{
    {Payload: data1, TTL: 1 * time.Minute},
    {Payload: data2, TTL: 5 * time.Minute},
    {Payload: data3, TTL: 0},  // Permanent
}
q.EnqueueBatchWithOptions(messages)
```

### 4. Automatic Expiration

```go
// Expired messages are automatically skipped during dequeue
msg, err := q.Dequeue()
// Returns next non-expired message
// No manual expiration check needed!
```

## How It Works

```
EnqueueWithTTL(payload, 10s)
         ↓
  ExpiresAt = Now() + 10s
         ↓
   Write to disk
         ↓
Dequeue() checks: Now() < ExpiresAt?
         ↓              ↓
       YES            NO
         ↓              ↓
    Return msg    Skip (expired)
                       ↓
                Find next valid message
```

**Storage**: ExpiresAt timestamp (8 bytes) stored with message if TTL > 0.

## Use Cases

### 1. Session Tokens

```go
// Token valid for 30 minutes
token := generateSessionToken(userID)
q.EnqueueWithTTL(token, 30*time.Minute)

// Consumer automatically skips expired tokens
msg, _ := q.Dequeue()
// msg.Payload is guaranteed valid (not expired)
```

### 2. Temporary Notifications

```go
// Email notification expires after 1 hour
notification := EmailNotification{
    To: "user@example.com",
    Subject: "Your order is ready for pickup",
}
q.EnqueueWithTTL(toJSON(notification), 1*time.Hour)

// If user already picked up order, notification expires automatically
```

### 3. Real-Time Analytics Events

```go
// Click events only relevant for 5 minutes
clickEvent := ClickEvent{
    UserID: "user-123",
    URL: "/products/abc",
    Timestamp: time.Now(),
}
q.EnqueueWithTTL(toJSON(clickEvent), 5*time.Minute)

// Batch processor skips stale events
```

### 4. Cache Invalidation

```go
// Invalidate cache entry after 10 seconds
cacheInvalidate := CacheKey{Key: "user:123:profile"}
q.EnqueueWithTTL(toJSON(cacheInvalidate), 10*time.Second)

// Consumer applies invalidation if still relevant
```

### 5. Time-Sensitive Offers

```go
// Flash sale offer valid for 1 hour
offer := FlashSale{
    ProductID: "xyz",
    Discount: 50,
    ValidUntil: time.Now().Add(1 * time.Hour),
}
q.EnqueueWithTTL(toJSON(offer), 1*time.Hour)
```

### 6. Temporary Monitoring Alerts

```go
// Alert expires after being active for 15 minutes
alert := Alert{
    Severity: "warning",
    Message: "High CPU usage detected",
    Threshold: 80,
}
q.EnqueueWithTTL(toJSON(alert), 15*time.Minute)

// Alert processor ignores if condition already resolved
```

## Combining with Other Features

### TTL + Headers

```go
headers := map[string]string{
    "type": "notification",
    "channel": "email",
}
opts := ledgerq.BatchEnqueueOptions{
    Payload: data,
    Headers: headers,
    TTL:     10 * time.Minute,
}
q.EnqueueBatchWithOptions([]ledgerq.BatchEnqueueOptions{opts})
```

### TTL + Priority

```go
// High-priority temporary alert
opts := ledgerq.BatchEnqueueOptions{
    Payload:  alert,
    Priority: ledgerq.PriorityHigh,
    TTL:      5 * time.Minute,
}
q.EnqueueBatchWithOptions([]ledgerq.BatchEnqueueOptions{opts})
```

## Performance

- **Enqueue overhead**: ~100 nanoseconds (calculate expiration timestamp)
- **Dequeue overhead**: ~50 nanoseconds per expired message (timestamp check)
- **Storage**: 8 bytes per message (ExpiresAt field)
- **Disk cleanup**: Expired messages cleaned up during compaction

**Expiration check**: O(1) timestamp comparison during dequeue.

## Best Practices

**✅ DO:**
- Use TTL for time-sensitive data (sessions, notifications, cache)
- Set TTL slightly longer than expected consumption time
- Monitor `PendingMessages` - high count may indicate expiration issues
- Use permanent messages for critical data

**❌ DON'T:**
- Use extremely short TTLs (<100ms) - high expiration overhead
- Rely on TTL for security (messages still on disk until compacted)
- Use TTL as primary data deletion mechanism (use compaction)
- Set TTL on mission-critical messages

## Troubleshooting

**Messages expiring too soon?**
- Check system clock synchronization
- Verify TTL calculation: `time.Now().Add(duration)`
- Ensure consumers are keeping up with enqueue rate

**Messages not expiring?**
- Verify TTL > 0 when enqueuing
- Check dequeue is being called (expiration checked on dequeue)
- Expired messages stay on disk until compaction

**High disk usage from expired messages?**
- Run compaction periodically: `q.Compact()`
- Check `SegmentCount` in stats
- Adjust `MaxSegmentSize` to trigger more frequent compaction

## Monitoring

```go
stats := q.Stats()

// High pending count with TTL may indicate expiration
if stats.PendingMessages > 10000 {
    // Many messages waiting - check expiration rate
}

// Check dequeue rate vs enqueue rate
enqueueRate := stats.TotalMessages / uptimeSeconds
dequeueRate := (stats.TotalMessages - stats.PendingMessages) / uptimeSeconds

if dequeueRate < enqueueRate {
    // Falling behind - messages may expire before processing
}
```

## Persistence

**TTL survives restarts**:
- ExpiresAt timestamp written to disk
- Expiration checked on every dequeue after restart
- No background expiration process needed

**Disk cleanup**:
- Expired messages remain on disk until compaction
- Use `Compact()` to reclaim space
- Compaction skips expired messages automatically

## Advanced: Custom Expiration Logic

For complex expiration scenarios, combine TTL with headers:

```go
// Enqueue with business logic expiration
headers := map[string]string{
    "expires-if": "order-cancelled",  // Business rule
    "order-id": "order-123",
}
opts := ledgerq.BatchEnqueueOptions{
    Payload: orderEvent,
    Headers: headers,
    TTL:     24 * time.Hour,  // Max time limit
}
q.EnqueueBatchWithOptions([]ledgerq.BatchEnqueueOptions{opts})

// Consumer checks both TTL and business logic
msg, _ := q.Dequeue()
if isOrderCancelled(msg.Headers["order-id"]) {
    q.Ack(msg.ID)  // Skip processing
    return
}
// Process event...
```

## Next Steps

- **[headers](../headers/)** - Add metadata to expiring messages
- **[priority](../priority/)** - Prioritize time-sensitive messages
- **[simple](../simple/)** - Basic usage without TTL

---

**Difficulty**: 🟢 Beginner | **Version**: v1.0.0+ | **Use Case**: Sessions, caches, temporary data
