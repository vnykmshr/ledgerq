# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

<!-- Add changes here as they are implemented -->

---

## [1.1.0] - 2025-10-19

### Added

#### Priority Queue Feature
- **Priority Levels** - Three-level priority system (High, Medium, Low) for message ordering
- **Priority-Aware Dequeue** - Messages are dequeued in priority order (High → Medium → Low) with FIFO within each level
- **Starvation Prevention** - Automatic promotion of old low-priority messages after configurable time window (default 30s)
- **Priority Index** - Efficient O(log n) binary search within each priority level using separate sorted slices
- **Dual Mode Operation** - FIFO mode (default) or Priority mode via `EnablePriorities` option for backward compatibility
- **Priority API** - New methods: `EnqueueWithPriority()` and `EnqueueWithAllOptions()` (priority + TTL + headers)
- **Batch Operations with Priorities** - `EnqueueBatchWithOptions()` supports per-message priority, TTL, and headers with single fsync efficiency
- **Priority Persistence** - Priority index is rebuilt from segments on queue restart
- **Comprehensive Testing** - 16 new priority queue tests (single + batch) and 11 performance benchmarks
- **Priority Example** - Complete example program demonstrating all priority features

#### API Additions
- `Priority` type (uint8): `PriorityLow` (0), `PriorityMedium` (1), `PriorityHigh` (2)
- `EnqueueWithPriority(payload []byte, priority Priority) (uint64, error)` - Enqueue with specific priority
- `EnqueueWithAllOptions(payload []byte, opts EnqueueOptions) (uint64, error)` - Enqueue with all features
- `EnqueueOptions` struct with `Priority`, `TTL`, and `Headers` fields
- `BatchEnqueueOptions` struct - Per-message options for batch operations
- `EnqueueBatchWithOptions(messages []BatchEnqueueOptions) ([]uint64, error)` - Batch enqueue with per-message priority, TTL, and headers
- `Options.EnablePriorities` (bool) - Enable priority queue mode (default: false)
- `Options.PriorityStarvationWindow` (time.Duration) - Time before low-priority promotion (default: 30s)
- `Message.Priority` field - Priority level of dequeued message

### Changed
- Entry format now includes optional priority byte when `EntryFlagPriority` is set
- Dequeue behavior: when `EnablePriorities=true`, returns highest priority message first
- `EnqueueBatch()` documentation updated to note default priority behavior (PriorityLow)
- Version constant updated to `1.1.0`

### Performance
- Priority queue overhead: ~1% compared to FIFO mode
- Priority enqueue: ~4.3 µs/op
- Priority dequeue: ~750 µs/op
- Priority index rebuild: ~120 µs for 1K messages, ~1.2 ms for 10K messages

### Fixed
- Fixed bug where `EnablePriorities` and `PriorityStarvationWindow` options were not passed through in public API `Open()` method

---

## [1.0.0] - 2025-10-18

### Added

#### Core Queue Features
- Persistent, disk-backed message queue with FIFO ordering guarantees
- Automatic segment rotation (configurable by size, count, or both)
- Read position persistence across queue restarts
- Crash-safe durability with append-only log design
- Thread-safe concurrent access for multiple producers and consumers
- Efficient batch operations (EnqueueBatch/DequeueBatch) with single fsync
- Sparse indexing for fast lookups with minimal overhead

#### Advanced Features
- **Streaming API** - Real-time push-based message delivery with context support
- **Message TTL (Time-To-Live)** - Automatic message expiration with lazy evaluation during dequeue
- **Message Headers** - Key-value metadata for routing, tracing, event sourcing, and workflow orchestration
- **Replay Capabilities** - Seek by message ID or timestamp for event replay
- **Compaction & Retention** - Automatic background or manual cleanup with configurable retention policies
- **Metrics Collection** - Zero-dependency in-memory metrics for monitoring and observability
- **Pluggable Logging** - Optional structured logging with custom logger interface

#### Public API
- Clean, stable public API package (`pkg/ledgerq`)
- Comprehensive configuration via Options pattern with sensible defaults
- Statistics API exposing queue state and performance metrics
- Context-aware streaming with graceful shutdown support

#### Developer Experience
- **CLI Tool** - Command-line tool for queue inspection, statistics, compaction, and peeking
- **Comprehensive Examples** - 7 runnable examples covering all major features
- **Fuzzing Tests** - Go 1.18+ fuzzing for format parsers and queue operations
- **Extensive Test Coverage** - >75% coverage across all core components
- **Race Detection** - Full test suite passes with -race flag
- **Comprehensive Documentation** - Complete README with usage examples, architecture details, and best practices

#### Testing & Quality
- 100+ test cases covering core functionality, edge cases, and error scenarios
- Integration tests for multi-segment scenarios and crash recovery
- Benchmarks for performance measurement and optimization
- Fuzzing tests for robustness (entry format, queue operations)
- Property-based testing for invariants

#### Project Infrastructure
- Go module with semantic versioning
- Makefile with development tasks (build, test, lint, bench, fuzz)
- CI/CD configuration (ready for GitHub Actions)
- Linter configuration (golangci-lint)
- Project documentation (CONTRIBUTING, CODE_OF_CONDUCT, SECURITY)
- Apache 2.0 license

### Changed
- N/A (initial release)

### Performance Characteristics
- Single operations: ~300-400 ns/op enqueue (without sync)
- Batch operations: ~200 ns per message (10x improvement)
- With AutoSync: ~19 ms/op (includes fsync)
- Concurrent: Excellent scalability with 8+ writers
- Dequeue: ~700 μs/op (with disk read)

### Design Highlights
- Zero dependencies beyond Go standard library
- Pure Go implementation (no C dependencies or external tools)
- Segment-based storage for efficient compaction
- Backward-compatible format extensions via feature flags
- Optional features with zero overhead when unused

---

## Template for Future Releases

<!--
## [X.Y.Z] - YYYY-MM-DD

### Added
- New features

### Changed
- Changes in existing functionality

### Deprecated
- Soon-to-be removed features

### Removed
- Removed features

### Fixed
- Bug fixes

### Security
- Security fixes
-->
