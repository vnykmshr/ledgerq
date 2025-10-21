# Metrics and Observability Example

Monitor queue performance with built-in metrics for production deployments.

## What You'll Learn

- Using the Metrics API
- Operation counters (enqueue, dequeue, errors)
- Payload size tracking
- Duration percentiles (P50, P95, P99)
- Queue state monitoring
- Compaction metrics

## Running the Example

```bash
go run main.go
```

## Sample Output

```
=== LedgerQ Metrics Example ===

Enqueueing 10 individual messages...
Enqueueing a batch of 5 messages...
Dequeuing 5 messages...
Dequeuing a batch of 3 messages...
Seeking to message ID 10...

=== Metrics Snapshot ===

Queue Name: example_queue

Operation Counters:
  Enqueue Total:       15
  Enqueue Batch Count: 1
  Dequeue Total:       8
  Dequeue Batch Count: 1
  Seek Operations:     1

Error Counters:
  Enqueue Errors: 0
  Dequeue Errors: 0

Payload Metrics:
  Total Bytes Enqueued: 166
  Total Bytes Dequeued: 103

Duration Percentiles:
  Enqueue P50: 5µs
  Enqueue P95: 5µs
  Enqueue P99: 5µs
  Dequeue P50: 500µs
  Dequeue P95: 5ms
  Dequeue P99: 5ms

Queue State:
  Pending Messages: 6
  Segment Count:    2
  Next Message ID:  16
  Read Message ID:  10

Compaction Metrics:
  Compactions Total: 0
  Segments Removed:  0
  Bytes Freed:       0
  Compaction Errors: 0
```

## Key Concepts

### 1. Access Metrics

```go
metrics := q.Metrics()

// Operation counts
fmt.Printf("Total enqueues: %d\n", metrics.EnqueueCount)
fmt.Printf("Total dequeues: %d\n", metrics.DequeueCount)

// Error tracking
fmt.Printf("Enqueue errors: %d\n", metrics.EnqueueErrors)
fmt.Printf("Dequeue errors: %d\n", metrics.DequeueErrors)
```

### 2. Performance Monitoring

```go
// Duration percentiles (in microseconds)
fmt.Printf("Enqueue P50: %dµs\n", metrics.EnqueueDurationP50)
fmt.Printf("Enqueue P95: %dµs\n", metrics.EnqueueDurationP95)
fmt.Printf("Enqueue P99: %dµs\n", metrics.EnqueueDurationP99)

fmt.Printf("Dequeue P50: %dµs\n", metrics.DequeueDurationP50)
fmt.Printf("Dequeue P95: %dµs\n", metrics.DequeueDurationP95)
fmt.Printf("Dequeue P99: %dµs\n", metrics.DequeueDurationP99)
```

### 3. Throughput Tracking

```go
// Payload byte counts
fmt.Printf("Bytes enqueued: %d\n", metrics.BytesEnqueued)
fmt.Printf("Bytes dequeued: %d\n", metrics.BytesDequeued)

// Calculate throughput
uptime := time.Since(startTime).Seconds()
throughput := float64(metrics.BytesDequeued) / uptime / 1024 / 1024
fmt.Printf("Throughput: %.2f MB/s\n", throughput)
```

### 4. Queue Health

```go
stats := q.Stats()

// Backlog monitoring
backlog := stats.PendingMessages
if backlog > 10000 {
    alert.Send("Queue backlog: %d messages", backlog)
}

// Consumer lag
lag := stats.NextMessageID - stats.ReadMessageID
if lag > 1000 {
    alert.Send("Consumer lag: %d messages", lag)
}
```

## Available Metrics

### Operation Counters

| Metric | Description | Type |
|--------|-------------|------|
| `EnqueueCount` | Total individual enqueue calls | Counter |
| `EnqueueBatchCount` | Total batch enqueue calls | Counter |
| `DequeueCount` | Total individual dequeue calls | Counter |
| `DequeueBatchCount` | Total batch dequeue calls | Counter |
| `SeekCount` | Total seek operations | Counter |

### Error Counters

| Metric | Description | Type |
|--------|-------------|------|
| `EnqueueErrors` | Failed enqueue operations | Counter |
| `DequeueErrors` | Failed dequeue operations | Counter |

### Payload Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| `BytesEnqueued` | Total bytes written | Bytes |
| `BytesDequeued` | Total bytes read | Bytes |

### Duration Percentiles

| Metric | Description | Unit |
|--------|-------------|------|
| `EnqueueDurationP50` | Median enqueue latency | Microseconds |
| `EnqueueDurationP95` | 95th percentile enqueue | Microseconds |
| `EnqueueDurationP99` | 99th percentile enqueue | Microseconds |
| `DequeueDurationP50` | Median dequeue latency | Microseconds |
| `DequeueDurationP95` | 95th percentile dequeue | Microseconds |
| `DequeueDurationP99` | 99th percentile dequeue | Microseconds |

### Queue State (from Stats)

| Metric | Description | Type |
|--------|-------------|------|
| `TotalMessages` | All messages ever enqueued | Counter |
| `PendingMessages` | Unread messages waiting | Gauge |
| `SegmentCount` | Number of data files | Gauge |
| `NextMessageID` | Next ID to be assigned | Gauge |
| `ReadMessageID` | Current read position | Gauge |

### Compaction Metrics

| Metric | Description | Type |
|--------|-------------|------|
| `CompactionCount` | Total compactions run | Counter |
| `SegmentsRemoved` | Segments deleted | Counter |
| `BytesFreed` | Disk space reclaimed | Counter (Bytes) |
| `CompactionErrors` | Failed compactions | Counter |

## Monitoring Patterns

### 1. Prometheus Integration

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    enqueueCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "ledgerq_enqueue_total",
        Help: "Total enqueue operations",
    })

    dequeueCounter = promauto.NewCounter(prometheus.CounterOpts{
        Name: "ledgerq_dequeue_total",
        Help: "Total dequeue operations",
    })

    pendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "ledgerq_pending_messages",
        Help: "Number of pending messages",
    })
)

func updatePrometheusMetrics(q *ledgerq.LedgerQ) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        metrics := q.Metrics()
        stats := q.Stats()

        enqueueCounter.Add(float64(metrics.EnqueueCount))
        dequeueCounter.Add(float64(metrics.DequeueCount))
        pendingGauge.Set(float64(stats.PendingMessages))
    }
}
```

### 2. Alerting Thresholds

```go
func checkAlerts(q *ledgerq.LedgerQ) {
    metrics := q.Metrics()
    stats := q.Stats()

    // High error rate
    errorRate := float64(metrics.EnqueueErrors+metrics.DequeueErrors) /
                 float64(metrics.EnqueueCount+metrics.DequeueCount)
    if errorRate > 0.01 {  // 1% error rate
        alert.Critical("Queue error rate: %.2f%%", errorRate*100)
    }

    // High latency
    if metrics.EnqueueDurationP99 > 10000 {  // 10ms
        alert.Warning("High enqueue latency: %dµs", metrics.EnqueueDurationP99)
    }

    // Queue backlog
    if stats.PendingMessages > 100000 {
        alert.Warning("High backlog: %d messages", stats.PendingMessages)
    }

    // Compaction failures
    if metrics.CompactionErrors > 0 {
        alert.Error("Compaction errors detected: %d", metrics.CompactionErrors)
    }
}
```

### 3. Performance Dashboard

```go
func logDashboard(q *ledgerq.LedgerQ) {
    metrics := q.Metrics()
    stats := q.Stats()

    fmt.Println("=== LedgerQ Dashboard ===")
    fmt.Printf("Throughput:\n")
    fmt.Printf("  Enqueues/sec: %.2f\n", float64(metrics.EnqueueCount)/uptime)
    fmt.Printf("  Dequeues/sec: %.2f\n", float64(metrics.DequeueCount)/uptime)

    fmt.Printf("\nLatency (P50/P95/P99):\n")
    fmt.Printf("  Enqueue: %dµs / %dµs / %dµs\n",
        metrics.EnqueueDurationP50,
        metrics.EnqueueDurationP95,
        metrics.EnqueueDurationP99)
    fmt.Printf("  Dequeue: %dµs / %dµs / %dµs\n",
        metrics.DequeueDurationP50,
        metrics.DequeueDurationP95,
        metrics.DequeueDurationP99)

    fmt.Printf("\nQueue Health:\n")
    fmt.Printf("  Pending: %d\n", stats.PendingMessages)
    fmt.Printf("  Segments: %d\n", stats.SegmentCount)
    fmt.Printf("  Error rate: %.2f%%\n", errorRate*100)
}
```

### 4. Capacity Planning

```go
func capacityAnalysis(q *ledgerq.LedgerQ) {
    metrics := q.Metrics()
    stats := q.Stats()

    avgPayloadSize := float64(metrics.BytesEnqueued) / float64(metrics.EnqueueCount)
    messagesPerSeg := float64(stats.TotalMessages) / float64(stats.SegmentCount)

    fmt.Printf("Capacity Analysis:\n")
    fmt.Printf("  Avg payload size: %.2f KB\n", avgPayloadSize/1024)
    fmt.Printf("  Messages per segment: %.0f\n", messagesPerSeg)
    fmt.Printf("  Disk usage estimate: %.2f MB\n",
        float64(stats.SegmentCount)*100/1024/1024)  // Assuming 100MB segments

    // Predict growth
    enqueueRate := float64(metrics.EnqueueCount) / uptime
    dequeueRate := float64(metrics.DequeueCount) / uptime
    growthRate := enqueueRate - dequeueRate

    if growthRate > 0 {
        timeToFull := float64(diskCapacity) / (growthRate * avgPayloadSize)
        fmt.Printf("  Time to disk full: %.1f hours\n", timeToFull/3600)
    }
}
```

## Best Practices

**✅ DO:**
- Poll metrics periodically (every 10-60 seconds)
- Export to monitoring systems (Prometheus, Datadog, etc.)
- Set up alerts for high error rates
- Track percentile latencies, not just averages
- Monitor both queue and system metrics (CPU, disk)

**❌ DON'T:**
- Poll metrics in tight loops (expensive)
- Ignore compaction errors
- Only monitor total counts (watch rates too)
- Alert on P50 latency (use P95/P99 instead)
- Forget to reset counters after export (if needed)

## Troubleshooting

**Metrics not updating?**
- Check you're calling `q.Metrics()`, not caching old result
- Verify operations are actually running

**High P99 latency spikes?**
- Check disk I/O (use `iostat`)
- Look for GC pauses (enable GC logging)
- Verify segment size not too large

**Enqueue/Dequeue count mismatch?**
- Normal - pending messages in queue
- Check `stats.PendingMessages`
- Verify consumers are running

## Example: Complete Monitoring Setup

```go
func monitorQueue(ctx context.Context, q *ledgerq.LedgerQ) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    startTime := time.Now()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            metrics := q.Metrics()
            stats := q.Stats()
            uptime := time.Since(startTime).Seconds()

            // Calculate rates
            enqueueRate := float64(metrics.EnqueueCount) / uptime
            dequeueRate := float64(metrics.DequeueCount) / uptime
            errorRate := float64(metrics.EnqueueErrors+metrics.DequeueErrors) /
                        float64(metrics.EnqueueCount+metrics.DequeueCount)

            // Log metrics
            log.Printf("Metrics: enq=%.1f/s deq=%.1f/s pending=%d err=%.2f%% p99_enq=%dµs p99_deq=%dµs",
                enqueueRate,
                dequeueRate,
                stats.PendingMessages,
                errorRate*100,
                metrics.EnqueueDurationP99,
                metrics.DequeueDurationP99)

            // Check alerts
            if errorRate > 0.01 {
                alert.Warning("High error rate: %.2f%%", errorRate*100)
            }
            if stats.PendingMessages > 50000 {
                alert.Warning("High backlog: %d messages", stats.PendingMessages)
            }
            if metrics.EnqueueDurationP99 > 5000 {  // 5ms
                alert.Warning("High enqueue P99: %dµs", metrics.EnqueueDurationP99)
            }
        }
    }
}
```

## Next Steps

- **[simple](../simple/)** - Basic queue operations
- **[producer-consumer](../producer-consumer/)** - Monitor concurrent workloads
- **[dlq](../dlq/)** - Track failure rates with DLQ

---

**Difficulty**: 🟡 Intermediate | **Version**: v1.0.0+ | **Use Case**: Production monitoring
