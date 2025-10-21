# Message Headers (Metadata) Example

Attach key-value metadata to messages for routing, tracing, and versioning.

## What You'll Learn

- Adding headers to messages
- Message routing with header filtering
- Distributed tracing integration
- Event sourcing metadata patterns
- Workflow orchestration with headers
- Schema versioning

## Running the Example

```bash
go run main.go
```

## Key Concepts

### 1. Basic Headers

```go
headers := map[string]string{
    "content-type": "application/json",
    "destination": "service-a",
    "priority": "high",
}
q.EnqueueWithHeaders(payload, headers)
```

### 2. Read Headers on Dequeue

```go
msg, _ := q.Dequeue()
fmt.Printf("Content-Type: %s\n", msg.Headers["content-type"])

// Check if header exists
if dest, ok := msg.Headers["destination"]; ok {
    routeToService(dest, msg.Payload)
}
```

### 3. Combine with Other Features

```go
// Headers + TTL
opts := ledgerq.BatchEnqueueOptions{
    Payload: data,
    Headers: map[string]string{"type": "notification"},
    TTL:     10 * time.Minute,
}
q.EnqueueBatchWithOptions([]ledgerq.BatchEnqueueOptions{opts})
```

## Use Cases

### 1. Message Routing

**Pattern**: Route messages to different handlers based on headers.

```go
// Producer
q.EnqueueWithHeaders(payload, map[string]string{
    "content-type": "application/json",
    "destination": "service-a",
})

// Consumer
msg, _ := q.Dequeue()
switch msg.Headers["destination"] {
case "service-a":
    handleServiceA(msg.Payload)
case "service-b":
    handleServiceB(msg.Payload)
}
```

**Example output**:
```
Enqueued JSON message with routing headers at offset 64
Dequeued message: {"action": "process", "data": 123}
Headers:
  content-type: application/json
  destination: service-a
  priority: high
```

### 2. Distributed Tracing

**Pattern**: Propagate trace context across services.

```go
// Start trace
traceID := generateTraceID()
spanID := generateSpanID()

q.EnqueueWithHeaders(event, map[string]string{
    "trace-id": traceID,
    "span-id": spanID,
    "service-name": "payment-service",
})

// Consumer continues trace
msg, _ := q.Dequeue()
ctx := trace.NewContext(
    msg.Headers["trace-id"],
    msg.Headers["span-id"],
)
processWithTrace(ctx, msg.Payload)
```

**Example output**:
```
Event: payment processed
Trace ID: 550e8400-e29b-41d4-a716-446655440000
Span ID: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

### 3. Event Sourcing Metadata

**Pattern**: Track domain events with aggregate metadata.

```go
// Publish domain event
q.EnqueueWithHeaders(eventData, map[string]string{
    "event-type": "UserCreated",
    "aggregate-type": "User",
    "aggregate-id": "user-123",
    "event-version": "1.0",
})

// Event store consumer
msg, _ := q.Dequeue()
event := DomainEvent{
    Type:        msg.Headers["event-type"],
    AggregateID: msg.Headers["aggregate-id"],
    Version:     msg.Headers["event-version"],
    Data:        msg.Payload,
}
eventStore.Append(event)
```

**Example output**:
```
Event Type: UserCreated
Aggregate: User/user-123
Payload: {"userId": "user-123", "email": "user@example.com"}
```

### 4. Workflow Orchestration

**Pattern**: Chain processing steps with state tracking.

```go
// Enqueue workflow step
q.EnqueueWithHeaders(payload, map[string]string{
    "workflow-id": "order-123",
    "step": "validate-order",
    "next-step": "charge-payment",
    "retry-count": "0",
    "max-retries": "3",
})

// Process step and enqueue next
msg, _ := q.Dequeue()
if processStep(msg.Headers["step"], msg.Payload) {
    nextPayload := prepareNextStep(msg.Payload)
    q.EnqueueWithHeaders(nextPayload, map[string]string{
        "workflow-id": msg.Headers["workflow-id"],
        "step": msg.Headers["next-step"],
        "next-step": "ship-order",
    })
}
```

**Example output**:
```
Processing workflow:
  Executing step 1: validate-order
    Next: 2, Retries: 0/3
  Executing step 2: charge-payment
    Next: 3, Retries: 0/5
  Executing step 3: ship-order
    Next: done, Retries: 0/3
```

### 5. Message Filtering

**Pattern**: Consumer-side filtering using headers.

```go
// Enqueue with classification
q.EnqueueWithHeaders(payload, map[string]string{
    "type": "audit",
    "region": "us-east",
})

// Filter on consume
msg, _ := q.Dequeue()
if msg.Headers["type"] != "audit" {
    continue  // Skip non-audit messages
}
processAuditLog(msg.Payload, msg.Headers["region"])
```

**Example output**:
```
Filtering messages (only 'audit' type):
  ✓ Matched: User logged in [region: us-east]
  ✗ Skipped: Payment received [type: billing]
  ✓ Matched: User logged out [region: us-east]
  ✗ Skipped: Invoice generated [type: billing]
```

### 6. Schema Versioning

**Pattern**: Version-aware message processing.

```go
// Producer
q.EnqueueWithHeaders(payload, map[string]string{
    "schema-version": "2.0",
    "schema-name": "UserEvent",
})

// Consumer with version handling
msg, _ := q.Dequeue()
switch msg.Headers["schema-version"] {
case "1.0":
    parseV1(msg.Payload)
case "2.0":
    parseV2(msg.Payload)  // Extended fields
default:
    return fmt.Errorf("unsupported version")
}
```

**Example output**:
```
Processing messages with version-aware handling:
  Schema v1.0: {"user": "john"}
    Using v1 parser
  Schema v2.0: {"userId": "john", "timestamp": 1234567890}
    Using v2 parser with extended fields
```

## How It Works

```
EnqueueWithHeaders(payload, headers)
         ↓
  Serialize headers (length-prefixed)
         ↓
  Write to disk: [Payload][HeaderCount][Key1Len][Key1][Val1Len][Val1]...
         ↓
Dequeue() reads and deserializes
         ↓
  msg.Headers map[string]string
```

**Storage**: Headers stored inline with message. ~20 bytes overhead per header (length prefixes + key/value).

## Performance

- **Enqueue overhead**: ~1-2 microseconds per header (serialization)
- **Dequeue overhead**: ~1-2 microseconds per header (deserialization)
- **Storage**: ~20 bytes + len(key) + len(value) per header
- **Zero overhead**: Messages without headers have no overhead

**Example**: 5 headers × 20 bytes each = ~100 bytes extra per message.

## Best Practices

**✅ DO:**
- Use short, lowercase header keys ("trace-id", not "X-Distributed-Trace-ID")
- Keep header values small (<100 bytes typical)
- Use headers for routing and metadata, not business data
- Standardize header names across services
- Document header semantics in team wiki

**❌ DON'T:**
- Store large data in headers (use payload instead)
- Use headers for sensitive data (no encryption)
- Create unbounded header counts (each adds overhead)
- Mix header purposes (separate tracing from routing)
- Use special characters in keys (stick to `[a-z0-9-]`)

## Header Naming Conventions

| Purpose | Example Headers | Values |
|---------|----------------|--------|
| Routing | `destination`, `queue-name` | `"service-a"`, `"billing"` |
| Tracing | `trace-id`, `span-id`, `parent-span-id` | UUIDs |
| Versioning | `schema-version`, `api-version` | `"1.0"`, `"2.0"` |
| Workflow | `workflow-id`, `step`, `next-step` | `"order-123"`, `"validate"` |
| Event Sourcing | `event-type`, `aggregate-id`, `aggregate-type` | `"UserCreated"`, `"user-123"` |
| Classification | `type`, `category`, `priority` | `"audit"`, `"billing"`, `"high"` |

## Troubleshooting

**Headers not persisting?**
- Check you're using `EnqueueWithHeaders()`, not `Enqueue()`
- Verify queue was closed cleanly (deferred `Close()`)

**High memory usage?**
- Count total headers: `len(msg.Headers)` × message count
- Reduce header count or value sizes
- Consider moving large data to payload

**Filtering slow?**
- Headers checked in-memory during `Dequeue()`
- For high-throughput filtering, use separate queues
- Example: `audit-queue`, `billing-queue` instead of one queue with type header

## Advanced Patterns

### Dynamic Routing

```go
// Route based on header
msg, _ := q.Dequeue()
queue := msg.Headers["target-queue"]
targetQ, _ := ledgerq.Open(fmt.Sprintf("/queues/%s", queue), nil)
targetQ.Enqueue(msg.Payload)
```

### Header Inheritance

```go
// Pass headers through pipeline
msg, _ := inputQ.Dequeue()
msg.Headers["processed-by"] = "stage-1"
outputQ.EnqueueWithHeaders(transformedPayload, msg.Headers)
```

### Conditional Processing

```go
msg, _ := q.Dequeue()
if msg.Headers["priority"] == "high" {
    processImmediately(msg.Payload)
} else {
    batchQueue.Enqueue(msg.Payload)  // Batch low-priority
}
```

## Next Steps

- **[ttl](../ttl/)** - Combine headers with time-to-live
- **[priority](../priority/)** - Use headers with priority ordering
- **[dlq](../dlq/)** - Headers preserved in DLQ (see `dlq.*` headers)

---

**Difficulty**: 🟢 Beginner | **Version**: v1.0.0+ | **Use Case**: Routing, tracing, event sourcing
