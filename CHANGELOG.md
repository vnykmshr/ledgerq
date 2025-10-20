# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

<!-- Add changes here as they are implemented -->

---

## [1.2.1] - 2025-10-20

### Changed

#### Code Organization and Maintainability
- **Major Internal Refactoring** - Reorganized `internal/queue` package for improved maintainability
  - Split monolithic `queue.go` (2,411 lines) into 9 focused modules (342 lines core + 8 feature modules)
  - Created dedicated files: `enqueue.go`, `dequeue.go`, `dlq.go`, `priority.go`, `stream.go`, `seek.go`, `lifecycle.go`, `options.go`, `validation.go`
  - Reorganized test files to mirror implementation structure
  - Reduced `queue_test.go` from 507 to 111 lines, created 4 focused test files
- **Documentation Updates**
  - Added comprehensive "Code Organization" section to ARCHITECTURE.md
  - Documented module responsibilities and design benefits
- **Test Coverage** - Maintained 76.9% test coverage throughout refactoring
- **Quality Assurance** - All tests passing, race detector clean

### Technical Details
- **No API Changes** - Public API (`pkg/ledgerq`) remains unchanged and fully compatible
- **No Breaking Changes** - This is an internal refactoring only
- **File Structure Benefits**:
  - Clear separation of concerns
  - Average file size ~200 lines (easy to understand)
  - Easy to locate and modify specific functionality
  - Reduced cognitive load for developers

---

## [1.2.0] - 2025-10-20

### Added

#### Dead Letter Queue (DLQ) Feature
- **Automatic Retry Tracking** - Track message processing failures with configurable max retry limit
- **Dead Letter Queue** - Separate queue for messages that exceed max retry attempts
- **Message Acknowledgment API** - Explicit `Ack()` and `Nack()` methods for message processing feedback
- **Crash-Safe Retry State** - Retry counters persist to disk using atomic JSON writes for crash recovery
- **Failure Metadata** - DLQ messages include original message ID, retry count, failure reason, and timestamp
- **DLQ Inspection** - Access DLQ messages via `GetDLQ()` method for investigation and monitoring
- **Message Requeuing** - Move messages from DLQ back to main queue after fixes via `RequeueFromDLQ()`
- **Zero Overhead When Disabled** - DLQ is opt-in; when disabled, Ack/Nack are no-ops with no performance impact
- **Infinite Recursion Prevention** - DLQ queue itself doesn't have a DLQ (DLQPath="" and MaxRetries=0)
- **Comprehensive Testing** - 12 new DLQ tests covering initialization, Ack/Nack, message movement, requeuing, and persistence
- **DLQ Example** - Complete example program demonstrating DLQ configuration, Ack/Nack, inspection, and requeuing

#### API Additions
- `Options.DLQPath` (string) - Path to dead letter queue directory (empty = disabled)
- `Options.MaxRetries` (int) - Maximum retry attempts before moving to DLQ (default: 3)
- `Queue.Ack(msgID uint64) error` - Acknowledge successful message processing
- `Queue.Nack(msgID uint64, reason string) error` - Report message processing failure
- `Queue.GetDLQ() *Queue` - Returns the dead letter queue for inspection (nil if not configured)
- `Queue.RequeueFromDLQ(dlqMsgID uint64) error` - Move message from DLQ back to main queue
- `Queue.GetRetryInfo(msgID uint64) *RetryInfo` - Returns retry information for implementing custom retry logic
- `RetryInfo` type - Contains MessageID, RetryCount, LastFailure, and FailureReason fields

#### DLQ Metadata Headers
DLQ messages automatically include the following headers:
- `dlq.original_msg_id` - Original message ID from main queue
- `dlq.retry_count` - Number of failed processing attempts
- `dlq.failure_reason` - Last failure reason from Nack()
- `dlq.last_failure` - Timestamp of last failure (RFC3339 format)

### Changed
- Version constant updated to `1.2.0`
- README updated with DLQ feature description and usage example
- USAGE.md documentation updated with comprehensive DLQ section
- Examples directory includes new DLQ example

### Implementation Details
- **RetryTracker Component** - Manages retry state with JSON persistence to `.retry_state.json`
- **Atomic State Writes** - Uses temp file + rename pattern for crash-safe retry state updates
- **Separate DLQ Queue** - DLQ is an independent Queue instance, enabling code reuse
- **Thread-Safe Operations** - All DLQ operations use proper mutex locking for concurrent access
- **Automatic Directory Creation** - DLQ directory is created automatically when DLQPath is configured
- **Backward Compatible** - DLQ disabled by default; existing code works without changes

### Use Cases
- Handling transient failures (network timeouts, temporary service outages)
- Poison message isolation (malformed data, processing bugs)
- Manual investigation of failed messages
- Automated retry with backoff strategies
- Message reprocessing after bug fixes

### Fixed
- **DLQ Tests** - Fixed 8 failing DLQ tests by correcting directory setup pattern (sibling directories instead of subdirectory)
- **Test Cleanup** - Removed 482 lines of redundant test code across priority, integration, and concurrent tests
- **Test Performance** - Improved test suite runtime by 22% (73s → 57s) through test consolidation
- **Documentation Accuracy** - Fixed incorrect dates and missing version information in SECURITY.md

### Improved
- **Test Quality** - Consolidated repetitive tests into table-driven patterns for better maintainability
- **Documentation Clarity** - Removed 141 lines of bloat and AI-generated content from USAGE.md
- **Architecture Documentation** - Added comprehensive priority queue and DLQ design sections to ARCHITECTURE.md
- **Contributor Experience** - Added new contributor checklist to CONTRIBUTING.md with onboarding steps
- **Retry Patterns** - Consolidated 3 redundant DLQ retry patterns into 1 recommended pattern with exponential backoff

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
