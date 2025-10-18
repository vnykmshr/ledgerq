# LedgerQ Architecture

Internal architecture and design decisions.

## Storage Format

Log-structured design with segment-based storage.

### Segment Files

Messages are stored in append-only `.log` files with companion `.idx` index files:

- **Naming**: Zero-padded base offsets like `00000000000000001000.log`
- **Rotation**: Automatic based on size, count, or time policies
- **Structure**: Each segment contains a sequence of entries in binary format

### Entry Format

Each entry consists of:

```
┌─────────┬──────┬───────┬────────────┬───────────┬───────────────┬─────────────┬─────────┬────────┐
│ Length  │ Type │ Flags │ Message ID │ Timestamp │ ExpiresAt (*) │ Headers (*) │ Payload │ CRC32  │
│  4 bytes│ 1 B  │  1 B  │   8 bytes  │  8 bytes  │   8 bytes     │  variable   │variable │ 4 bytes│
└─────────┴──────┴───────┴────────────┴───────────┴───────────────┴─────────────┴─────────┴────────┘

(*) Optional fields based on flags
```

**Size**: 26 bytes (base) + optional fields + payload

**Field Details**:
- **Length**: Total entry size (excluding length field itself)
- **Type**: Entry type (Data=1, Tombstone=2, Checkpoint=3)
- **Flags**: Feature flags (TTL, Headers, Compression, etc.)
- **Message ID**: Unique monotonic message identifier
- **Timestamp**: Unix nanoseconds when enqueued
- **ExpiresAt**: Unix nanoseconds when message expires (only if TTL flag set)
- **Headers**: Key-value pairs (only if Headers flag set)
  - Format: `NumHeaders (2B) + [KeyLen (2B) + Key + ValueLen (2B) + Value]...`
- **Payload**: Message data (variable length)
- **CRC32**: Checksum for corruption detection

### Index Format

Sparse indexes alongside segment files:

```
┌────────────────┬────────────┬───────────┬────────────┐
│ Magic (4 bytes)│ Version(4B)│ Count (4B)│  Entries   │
└────────────────┴────────────┴───────────┴────────────┘

Each index entry:
┌────────────┬───────────┬──────────────┐
│ Message ID │ Timestamp │ File Offset  │
│   8 bytes  │  8 bytes  │   8 bytes    │
└────────────┴───────────┴──────────────┘
```

**Indexing Strategy**:
- Index entry created every 4KB of segment data
- Lookup: Binary search index + linear scan of ~4KB
- Trade-off: Space efficiency vs lookup speed

### Metadata Format

Queue metadata is stored in `metadata.json`:

```json
{
  "version": 1,
  "next_message_id": 42,
  "read_message_id": 20,
  "created_at": 1234567890,
  "updated_at": 1234567900,
  "segment_count": 3
}
```

**Atomic Updates**: Metadata updates use write-rename pattern for atomicity.

## Architecture Layers

### 1. Public API Layer (`pkg/ledgerq/`)

Clean, stable public interface for external consumers:

```
Queue (public wrapper)
  ├── Enqueue/EnqueueBatch
  ├── Dequeue/DequeueBatch
  ├── EnqueueWithTTL/EnqueueWithHeaders
  ├── SeekToMessageID/SeekToTimestamp
  ├── Stream (with context)
  ├── Stats
  ├── Compact
  └── Close
```

**Responsibilities**:
- Type conversion between public and internal types
- Configuration via Options pattern
- Logger and metrics adapter patterns

### 2. Queue Layer (`internal/queue/`)

Core queue implementation with business logic:

```
Queue (internal)
  ├── Segment Manager (rotation, compaction)
  ├── Metadata Manager (persistent state)
  ├── Reader (dequeue, seek, scan)
  ├── Writer (enqueue, batch)
  ├── TTL Handler (expiration logic)
  ├── Compaction Timer
  └── Sync Manager
```

**Responsibilities**:
- Message enqueue/dequeue orchestration
- TTL expiration handling
- Read position tracking
- Automatic compaction scheduling
- Concurrent access coordination

### 3. Segment Layer (`internal/segment/`)

Low-level segment file management:

```
Segment Manager
  ├── Writer (active segment)
  ├── Reader (read any segment)
  ├── Naming (offset-based names)
  ├── Discovery (existing segments)
  └── Compaction (retention policies)
```

**Responsibilities**:
- File I/O operations
- Segment rotation based on policy
- Segment discovery and validation
- Retention policy enforcement

### 4. Format Layer (`internal/format/`)

Binary serialization and indexing:

```
Format
  ├── Entry (marshal/unmarshal)
  ├── Index (sparse indexing)
  ├── Metadata (JSON persistence)
  ├── Checksum (CRC32)
  └── Segment Header
```

**Responsibilities**:
- Binary encoding/decoding
- Checksum calculation and verification
- Index creation and searching
- Format version management

### 5. Support Layers

**Logging** (`internal/logging/`):
- Interface-based pluggable logging
- Noop and default implementations
- Structured field support

**Metrics** (`internal/metrics/`):
- Zero-dependency in-memory metrics
- Duration histograms (P50, P95, P99)
- Atomic counters for thread safety
- Snapshot-based reads

## Design Decisions

### 1. Segment-Based Storage

**Why?**
- **Efficient Compaction**: Remove old segments without rewriting entire queue
- **Bounded File Sizes**: Prevent single files from growing too large
- **Fast Startup**: Only scan last segment on recovery
- **Better OS Caching**: Hot segments cached, cold segments paged out

**Trade-offs**:
- More complex file management
- Need segment discovery on startup
- Small overhead for segment rotation

### 2. Read Position Persistence

**Why?**
- True queue semantics: consumed = permanently consumed
- No accidental message reprocessing after restart
- Predictable behavior for production systems

**Implementation**:
- Read position stored in metadata.json
- Updated on every dequeue (respects AutoSync setting)
- Hybrid recovery: metadata + segment scanning
- Seekable: explicit seeking updates persisted position

**Trade-offs**:
- Metadata I/O on every dequeue
- Mitigated by batching and async sync

### 3. Sparse Indexing

**Why?**
- **Space Efficient**: ~4KB index per 1MB segment (0.4% overhead)
- **Fast Enough**: Linear scan of 4KB is <1μs
- **Simple**: No complex B-tree or LSM maintenance

**Trade-offs**:
- Not optimal for random access patterns
- Designed for sequential dequeue workloads

### 4. TTL with Lazy Expiration

**Why?**
- No background cleanup threads
- Zero overhead for non-TTL messages
- Simple implementation

**How?**
- ExpiresAt timestamp stored with message
- Check on dequeue, skip if expired
- Eventually cleaned up by compaction

**Trade-offs**:
- Expired messages occupy disk until compaction
- Not suitable for strict TTL enforcement

### 5. Optional Features with Flags

**Why?**
- Backward compatibility
- Zero overhead when unused
- Extensible format

**Implementation**:
- Flags field in entry format
- Optional fields parsed based on flags
- Version field for breaking changes

## Concurrency Model

### Thread Safety

LedgerQ provides safe concurrent access with these guarantees:

**Queue Level**:
- Multiple concurrent enqueuers: Safe
- Multiple concurrent dequeuers: Safe (same read position)
- Mixed enqueue/dequeue: Safe

**Implementation**:
- `sync.RWMutex` for queue state
- Atomic operations for metrics
- File-level locking for metadata
- No locks held during I/O operations

**Independent Consumers**: Not supported (use separate queue instances)

### Sync Strategies

**AutoSync = false** (default):
- Writes buffered in OS
- Periodic fsync based on SyncInterval
- **Tradeoff**: Better throughput, risk of data loss on crash

**AutoSync = true**:
- fsync after every write
- Guaranteed durability
- **Tradeoff**: Slower (~50x), guaranteed persistence

**Batch Operations**:
- Single fsync for entire batch
- Best of both worlds for high throughput

## Performance Characteristics

### Enqueue Performance

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| Single (no sync) | ~3M ops/sec | 300-400 ns | Buffered writes |
| Single (AutoSync) | ~50 ops/sec | ~19 ms | Includes fsync |
| Batch 100 (no sync) | ~50M msgs/sec | 200 ns/msg | Amortized |
| Batch 100 (AutoSync) | ~5K msgs/sec | 200 μs/msg | Single fsync |

### Dequeue Performance

| Operation | Throughput | Latency | Notes |
|-----------|------------|---------|-------|
| Single | ~1.4K ops/sec | ~700 μs | Includes disk read |
| Batch 10 | ~14K msgs/sec | ~700 ns/msg | Amortized I/O |

### Scalability

**Concurrent Writers** (8 goroutines):
- Enqueue: ~350 ns/op
- Batch: ~2.2 μs/op

**Bottlenecks**:
- Single read position (dequeue serialization)
- fsync latency (when AutoSync enabled)
- Disk I/O bandwidth

## Recovery and Durability

### Crash Recovery

**On Open**:
1. Read metadata.json for last known state
2. Scan all segment files to rebuild state
3. Validate segment sequence (detect corruption)
4. Resume from persisted read position

**Corruption Handling**:
- CRC32 checksums detect corrupted entries
- Skip corrupted entries during recovery
- Log warnings for investigation

### Data Loss Scenarios

**AutoSync = false**:
- Data in OS buffer cache may be lost on crash
- Metadata may be slightly behind actual state
- Worst case: lose messages since last sync interval

**AutoSync = true**:
- No data loss on crash (fsync guarantees)
- Metadata always consistent with segments

## Compaction

### Retention Policies

Segments can be removed based on:

```go
RetentionPolicy{
    MaxAge:      7 * 24 * time.Hour,  // Age-based
    MaxSize:     10 * 1024 * 1024 * 1024,  // Size-based
    MaxSegments: 100,                  // Count-based
    MinSegments: 1,                    // Safety minimum
}
```

### Compaction Process

1. **Identify candidates**: Segments older than read position
2. **Apply retention**: Age, size, count limits
3. **Remove segments**: Delete .log and .idx files
4. **Update metadata**: Reflect new segment count

### Automatic Compaction

```go
opts.CompactionInterval = 5 * time.Minute
```

Background goroutine triggers compaction periodically.

### Manual Compaction

```go
result, err := q.Compact()
// Returns: SegmentsRemoved, BytesFreed
```

## Future Enhancements

Potential improvements for future versions:

1. **Compression**: Transparent payload compression
2. **Encryption**: At-rest encryption for sensitive data
3. **Replication**: Multi-node replication for HA
4. **Consumer Groups**: Multiple independent consumers
5. **Transactions**: Multi-message atomic operations
6. **Tiered Storage**: Hot/cold data separation

These would require format version bumps and breaking API changes.
