# Streaming Example

This example demonstrates real-time streaming message processing with LedgerQ.

## Features Demonstrated

- **Push-based streaming**: Messages are pushed to a handler as they become available
- **Context-based cancellation**: Graceful shutdown using Go contexts
- **Concurrent producer/consumer**: Producer adds messages while consumer streams them
- **Real-time processing**: Event-driven architecture with minimal polling overhead
- **Signal handling**: Clean shutdown on Ctrl+C

## Running the Example

```bash
go run main.go
```

The example will:
1. Start a streaming consumer that processes messages in real-time
2. Start a producer that creates messages every 500ms
3. Display both producer and consumer activity
4. Stop automatically after 10 messages (or press Ctrl+C)

## Sample Output

```
=== LedgerQ Streaming Example ===
Press Ctrl+C to stop

Starting streaming consumer...
Starting message producer...
[Producer] Enqueued message #1 at offset 64
[Consumer] Received message #1 (ID: 1): Event 1 at 15:04:05
[Producer] Enqueued message #2 at offset 106
[Consumer] Received message #2 (ID: 2): Event 2 at 15:04:06
...
Producer finished sending messages.
Consumer stopped. Processed 10 messages total.

=== Streaming Example Complete ===
```

## Key Concepts

### Stream Handler

The `StreamHandler` function is called for each message:

```go
handler := func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    return nil  // Return error to stop streaming
}
```

### Context Cancellation

Streaming stops gracefully when the context is cancelled:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

err := q.Stream(ctx, handler)
// Returns context.Canceled when cancelled
```

### Use Cases

- Real-time event processing
- Log streaming and analysis
- Message relay and forwarding
- Continuous data pipelines
- Event-driven architectures

## Comparison with Polling

**Traditional Polling:**
```go
for {
    msg, err := q.Dequeue()
    if err != nil {
        time.Sleep(100 * time.Millisecond)
        continue
    }
    process(msg)
}
```

**Streaming API:**
```go
q.Stream(ctx, func(msg *ledgerq.Message) error {
    process(msg)
    return nil
})
```

Benefits:
- Cleaner code
- Automatic error handling
- Built-in context support
- Consistent polling interval
- Less boilerplate
