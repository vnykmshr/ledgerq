# Producer-Consumer Concurrency Example

Demonstrate thread-safe concurrent access with multiple producers and consumers.

## What You'll Learn

- Multiple producers writing concurrently
- Multiple consumers reading concurrently
- Thread safety guarantees
- Goroutine-based patterns
- Work distribution across consumers

## Running the Example

```bash
go run main.go
```

## Sample Output

```
LedgerQ Producer-Consumer Example
==================================

✓ Queue opened at: /tmp/ledgerq-producer-consumer

Starting 3 producers (each will produce 10 messages)...
  [Producer 2] Starting...
  [Producer 1] Starting...
  [Producer 0] Starting...

Starting 2 consumers...
  [Consumer 1] Starting...
  [Consumer 0] Starting...
  [Consumer 1] Received [ID:1]: Message from Producer 2: #0
  [Consumer 0] Received [ID:2]: Message from Producer 1: #0
  [Consumer 0] Received [ID:3]: Message from Producer 0: #0
  [Consumer 1] Received [ID:4]: Message from Producer 2: #1
  [...]
  [Producer 2] Completed 10 messages
  [Producer 1] Completed 10 messages
  [Producer 0] Completed 10 messages
  [Consumer 0] Completed. Consumed 15 messages
  [Consumer 1] Completed. Consumed 15 messages

=== Results ===
Expected:  30 messages
Consumed:  30 messages

✓ All messages successfully processed!
```

## Key Concepts

### 1. Thread-Safe Enqueue

```go
var wg sync.WaitGroup

// Launch multiple producers
for i := 0; i < 3; i++ {
    wg.Add(1)
    go func(producerID int) {
        defer wg.Done()
        for j := 0; j < 10; j++ {
            msg := fmt.Sprintf("Message from Producer %d: #%d", producerID, j)
            q.Enqueue([]byte(msg))  // Thread-safe!
        }
    }(i)
}

wg.Wait()  // Wait for all producers to finish
```

**Thread safety**: LedgerQ uses internal mutexes - no external synchronization needed.

### 2. Thread-Safe Dequeue

```go
var wg sync.WaitGroup

// Launch multiple consumers
for i := 0; i < 2; i++ {
    wg.Add(1)
    go func(consumerID int) {
        defer wg.Done()
        for {
            msg, err := q.Dequeue()  // Thread-safe!
            if err == ledgerq.ErrNoMessages {
                break
            }
            process(msg.Payload)
        }
    }(i)
}

wg.Wait()  // Wait for all consumers to finish
```

**Guarantee**: Each message dequeued exactly once, even with multiple consumers.

### 3. Work Distribution

```go
// Messages automatically distributed across consumers
// No manual partitioning needed!

// Consumer 0 might get: [ID:1, ID:3, ID:5, ...]
// Consumer 1 might get: [ID:2, ID:4, ID:6, ...]
```

## Concurrency Patterns

### Pattern 1: Fixed Producer/Consumer Pools

```go
func producerConsumerPool(q *ledgerq.LedgerQ) {
    numProducers := 5
    numConsumers := 10

    var wg sync.WaitGroup

    // Producer pool
    for i := 0; i < numProducers; i++ {
        wg.Add(1)
        go producer(q, i, &wg)
    }

    // Consumer pool
    for i := 0; i < numConsumers; i++ {
        wg.Add(1)
        go consumer(q, i, &wg)
    }

    wg.Wait()
}

func producer(q *ledgerq.LedgerQ, id int, wg *sync.WaitGroup) {
    defer wg.Done()
    for j := 0; j < 100; j++ {
        q.Enqueue(generateWork(id, j))
    }
}

func consumer(q *ledgerq.LedgerQ, id int, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        msg, err := q.Dequeue()
        if err == ledgerq.ErrNoMessages {
            time.Sleep(10 * time.Millisecond)
            continue
        }
        processWork(msg.Payload)
    }
}
```

### Pattern 2: Dynamic Consumer Scaling

```go
// Add consumers dynamically based on queue depth
func autoScale(q *ledgerq.LedgerQ) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    consumers := make(map[int]context.CancelFunc)
    nextID := 0

    for range ticker.C {
        stats := q.Stats()
        targetConsumers := int(stats.PendingMessages / 100)  // 1 per 100 msgs

        // Scale up
        for len(consumers) < targetConsumers {
            ctx, cancel := context.WithCancel(context.Background())
            consumers[nextID] = cancel
            go scalableConsumer(ctx, q, nextID)
            nextID++
        }

        // Scale down
        for len(consumers) > targetConsumers {
            for id, cancel := range consumers {
                cancel()
                delete(consumers, id)
                break
            }
        }
    }
}

func scalableConsumer(ctx context.Context, q *ledgerq.LedgerQ, id int) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            msg, err := q.Dequeue()
            if err == ledgerq.ErrNoMessages {
                time.Sleep(10 * time.Millisecond)
                continue
            }
            process(msg.Payload)
        }
    }
}
```

### Pattern 3: Batch Producer

```go
func batchProducer(q *ledgerq.LedgerQ) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    buffer := make([][]byte, 0, 100)

    for range ticker.C {
        // Collect work into buffer
        work := collectPendingWork()  // Your business logic
        buffer = append(buffer, work...)

        // Batch enqueue when buffer full or timer fires
        if len(buffer) >= 100 {
            q.EnqueueBatch(buffer)
            buffer = buffer[:0]  // Clear buffer
        }
    }
}
```

### Pattern 4: Priority Consumer Groups

```go
// Different consumer groups for different priorities
func priorityConsumers(highQ, lowQ *ledgerq.LedgerQ) {
    var wg sync.WaitGroup

    // More consumers for high-priority queue
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go consumer(highQ, "high", i, &wg)
    }

    // Fewer consumers for low-priority queue
    for i := 0; i < 2; i++ {
        wg.Add(1)
        go consumer(lowQ, "low", i, &wg)
    }

    wg.Wait()
}
```

## Performance Characteristics

**Concurrent Enqueue**:
- ~200-300 ns overhead per operation (mutex acquisition)
- Scales linearly up to ~4-8 producers (depends on CPU cores)
- Beyond 8 producers: diminishing returns due to lock contention

**Concurrent Dequeue**:
- ~300-400 ns overhead per operation
- Best performance: 1-4 consumers
- More consumers → more contention on read position

**Optimal Configuration**:
```
High throughput:   Producers: 4-8, Consumers: 2-4
Low latency:       Producers: 1-2, Consumers: 4-8
Balanced:          Producers: 3, Consumers: 3
```

## Best Practices

**✅ DO:**
- Use WaitGroups to coordinate goroutine cleanup
- Handle `ErrNoMessages` gracefully (sleep/retry)
- Log goroutine lifecycle (start/stop)
- Monitor queue depth with `Stats()`
- Use batch operations when possible

**❌ DON'T:**
- Busy-wait in tight loops (waste CPU)
- Create unbounded goroutines (memory leak risk)
- Ignore `ErrNoMessages` (infinite loop)
- Mix multiple consumer logic in one goroutine
- Forget to call `Close()` on shutdown

## Error Handling

```go
func robustConsumer(q *ledgerq.LedgerQ, id int) {
    for {
        msg, err := q.Dequeue()
        if err != nil {
            if err == ledgerq.ErrNoMessages {
                time.Sleep(100 * time.Millisecond)
                continue
            }
            log.Printf("[Consumer %d] Fatal error: %v", id, err)
            return
        }

        // Process with recovery
        func() {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("[Consumer %d] Panic: %v", id, r)
                }
            }()
            process(msg.Payload)
        }()
    }
}
```

## Graceful Shutdown

```go
func gracefulShutdown(q *ledgerq.LedgerQ) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    var wg sync.WaitGroup

    // Start consumers with context
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for {
                select {
                case <-ctx.Done():
                    log.Printf("[Consumer %d] Shutting down gracefully", id)
                    return
                default:
                    msg, err := q.Dequeue()
                    if err == ledgerq.ErrNoMessages {
                        time.Sleep(100 * time.Millisecond)
                        continue
                    }
                    process(msg.Payload)
                }
            }
        }(i)
    }

    // Wait for shutdown signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    <-sigChan

    log.Println("Shutdown signal received, draining queue...")
    cancel()  // Signal goroutines to stop
    wg.Wait() // Wait for all consumers to finish

    q.Close()
    log.Println("Queue closed successfully")
}
```

## Monitoring

```go
func monitorQueue(q *ledgerq.LedgerQ, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for range ticker.C {
        stats := q.Stats()
        log.Printf("Queue Stats: Pending=%d, Total=%d, Segments=%d",
            stats.PendingMessages,
            stats.TotalMessages,
            stats.SegmentCount)

        // Alert if queue growing
        if stats.PendingMessages > 10000 {
            log.Printf("WARNING: Queue depth high (%d messages)", stats.PendingMessages)
        }
    }
}
```

## Troubleshooting

**Consumers starving (no messages)?**
- Check producers are running and enqueuing
- Verify no errors in producer goroutines
- Inspect `Stats().PendingMessages`

**High CPU usage?**
- Add sleep in consumer loop when `ErrNoMessages`
- Reduce number of consumer goroutines
- Use batch dequeue instead of individual

**Uneven work distribution?**
- This is normal - OS scheduling varies
- Some consumers may process more than others
- Total work completed should match expected

**Memory leak?**
- Check for goroutine leaks (unbounded spawning)
- Use pprof to profile goroutine count
- Ensure all goroutines exit on shutdown

## Next Steps

- **[simple](../simple/)** - Single-threaded basics
- **[priority](../priority/)** - Priority-based work distribution
- **[streaming](../streaming/)** - Push-based real-time processing

---

**Difficulty**: 🟢 Beginner | **Version**: v1.0.0+ | **Use Case**: High-throughput systems
