# Payload Compression Example (v1.3.0+)

Reduce disk usage with GZIP compression. Useful for large text payloads like JSON, XML, or logs.

## What You'll Learn

- Enabling default compression for all messages
- Per-message compression control
- Compression with other features (priority, headers)
- When to use compression

## Running

```bash
go run main.go
```

## Key Concepts

### 1. Enable Default Compression

```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DefaultCompression = ledgerq.CompressionGzip
opts.CompressionLevel = 6             // 1=fast, 9=best (default: 6)
opts.MinCompressionSize = 512         // Only compress >= 512 bytes
q, _ := ledgerq.Open("/path/to/queue", opts)

// Messages >= 512 bytes auto-compressed
q.Enqueue(largePayload)  // Compressed automatically
```

### 2. Per-Message Control

```go
// Force compression for this message
q.EnqueueWithCompression(payload, ledgerq.CompressionGzip)

// Explicitly disable compression
q.EnqueueWithCompression(payload, ledgerq.CompressionNone)
```

### 3. Combine with Other Features

```go
// Priority + Compression + Headers
messages := []ledgerq.BatchEnqueueOptions{
    {
        Payload:     largeJSONPayload,
        Priority:    ledgerq.PriorityHigh,
        Compression: ledgerq.CompressionGzip,
        Headers:     map[string]string{"type": "log"},
    },
}
q.EnqueueBatchWithOptions(messages)
```

## How It Works

```
Enqueue(payload)
     ↓
Size >= MinCompressionSize?
     ↓ Yes
GZIP Compress
     ↓
Savings >= 5%?
     ↓ Yes
Store compressed + set flag
     ↓
Dequeue() → Auto-decompress (transparent)
```

**Efficiency check**: Only stores compressed version if >= 5% smaller.

## When to Use

| Payload Type | Compression | Typical Savings |
|--------------|-------------|-----------------|
| JSON, XML | ✅ YES | 60-80% |
| Plain text, logs | ✅ YES | 50-70% |
| HTML, CSV | ✅ YES | 60-75% |
| Images (JPEG, PNG) | ❌ NO | 0-5% (pre-compressed) |
| Protobuf, Avro | ❌ NO | 10-20% (already compact) |
| Small messages (<1KB) | ❌ NO | Overhead > savings |

## Performance

- **Compression**: ~500 KB/s (level 6)
- **Decompression**: ~15 MB/s
- **Latency**: +2-3ms for 10KB payload

For 100KB JSON: ~200ms compression, ~7ms decompression.

## Best Practices

**✅ DO:**
- Use for text formats (JSON, XML, CSV)
- Set `MinCompressionSize` to skip small messages
- Use level 6 (balanced) or 1 (fast)
- Monitor disk savings with queue stats

**❌ DON'T:**
- Compress already-compressed data (images, video)
- Use level 9 (slow with minimal extra savings)
- Compress tiny messages (<512 bytes)
- Assume all payloads benefit equally

## Configuration Examples

**High throughput** (prioritize speed):
```go
opts.CompressionLevel = 1          // Fastest
opts.MinCompressionSize = 2048     // Only large messages
```

**Disk constrained** (maximize savings):
```go
opts.CompressionLevel = 9          // Best compression
opts.MinCompressionSize = 512      // Compress more messages
```

**Balanced** (recommended):
```go
opts.CompressionLevel = 6          // Default
opts.MinCompressionSize = 1024     // 1KB threshold
```

## Security

- **Decompression bomb protection**: 100MB limit
- Uses stdlib `compress/gzip` only (no external deps)
- No encryption (compress before encrypting if needed)

## Monitoring

```go
// Track uncompressed vs compressed sizes
originalSize := len(payload)
offset, _ := q.EnqueueWithCompression(payload, ledgerq.CompressionGzip)

// Check effectiveness in production
stats := q.Stats()
fmt.Printf("Segments: %d, Total size on disk: check filesystem\n", stats.SegmentCount)
```

## Next Steps

- **[deduplication](../deduplication/)** - Prevent duplicate processing
- **[priority](../priority/)** - Combine compression with priority ordering
- **[streaming](../streaming/)** - Process compressed messages in real-time

---

**Difficulty**: 🟢 Beginner | **Version**: v1.3.0+ | **Use Case**: Large payloads, log aggregation
