// Package ledgerq provides a persistent, disk-backed message queue.
//
// LedgerQ is designed for local message persistence with FIFO guarantees,
// automatic segment rotation, and efficient replay capabilities.
//
// Example usage:
//
//	q, err := ledgerq.Open("/path/to/queue", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Enqueue a message
//	offset, err := q.Enqueue([]byte("Hello, World!"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Dequeue a message
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Message: %s\n", msg.Payload)
package ledgerq

import (
	"context"
	"time"

	"github.com/vnykmshr/ledgerq/internal/logging"
	"github.com/vnykmshr/ledgerq/internal/metrics"
	"github.com/vnykmshr/ledgerq/internal/queue"
	"github.com/vnykmshr/ledgerq/internal/segment"
)

// Queue represents a persistent message queue.
type Queue struct {
	q *queue.Queue
}

// Message represents a dequeued message.
type Message struct {
	// ID is the unique message identifier
	ID uint64

	// Offset is the file offset where the message is stored
	Offset uint64

	// Payload is the message data
	Payload []byte

	// Timestamp is when the message was enqueued (Unix nanoseconds)
	Timestamp int64
}

// Options configures queue behavior.
type Options struct {
	// AutoSync enables automatic syncing after each write
	// Default: false
	AutoSync bool

	// SyncInterval for periodic syncing (if AutoSync is false)
	// Default: 1 second
	SyncInterval time.Duration

	// CompactionInterval for automatic background compaction
	// Set to 0 to disable automatic compaction (default)
	CompactionInterval time.Duration

	// MaxSegmentSize is the maximum size of a segment file in bytes
	// Default: 1GB
	MaxSegmentSize uint64

	// MaxSegmentMessages is the maximum number of messages per segment
	// Default: unlimited (0)
	MaxSegmentMessages uint64

	// RotationPolicy determines when to rotate segments
	// Options: RotateBySize, RotateByCount, RotateByBoth
	// Default: RotateBySize
	RotationPolicy RotationPolicy

	// RetentionPolicy configures segment retention and cleanup
	// Default: nil (no automatic cleanup)
	RetentionPolicy *RetentionPolicy

	// Logger for structured logging (nil = no logging)
	// Default: no logging
	Logger Logger

	// MetricsCollector for collecting queue metrics (nil = no metrics)
	// Default: no metrics
	MetricsCollector MetricsCollector
}

// MetricsCollector defines the interface for recording queue metrics.
type MetricsCollector interface {
	RecordEnqueue(payloadSize int, duration time.Duration)
	RecordDequeue(payloadSize int, duration time.Duration)
	RecordEnqueueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordDequeueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordEnqueueError()
	RecordDequeueError()
	RecordSeek()
	RecordCompaction(segmentsRemoved int, bytesFreed int64, duration time.Duration)
	RecordCompactionError()
	UpdateQueueState(pending, segments, nextMsgID, readMsgID uint64)
}

// RotationPolicy determines when segment rotation occurs.
type RotationPolicy int

const (
	// RotateBySize rotates when segment size exceeds MaxSegmentSize
	RotateBySize RotationPolicy = iota

	// RotateByCount rotates when message count exceeds MaxSegmentMessages
	RotateByCount

	// RotateByBoth rotates when either size or count limit is reached
	RotateByBoth
)

// RetentionPolicy configures segment retention.
type RetentionPolicy struct {
	// MaxAge is the maximum age of segments to keep
	MaxAge time.Duration

	// MaxSize is the maximum total size of all segments
	MaxSize uint64

	// MaxSegments is the maximum number of segments to keep
	MaxSegments int

	// MinSegments is the minimum number of segments to always keep
	MinSegments int
}

// Logger interface for pluggable logging.
type Logger interface {
	Debug(msg string, fields ...LogField)
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
}

// LogField represents a structured log field.
type LogField struct {
	Key   string
	Value interface{}
}

// Stats contains queue statistics.
type Stats struct {
	// TotalMessages is the total number of messages ever enqueued
	TotalMessages uint64

	// PendingMessages is the number of unread messages
	PendingMessages uint64

	// NextMessageID is the ID that will be assigned to the next enqueued message
	NextMessageID uint64

	// ReadMessageID is the ID of the next message to be dequeued
	ReadMessageID uint64

	// SegmentCount is the number of segments
	SegmentCount int
}

// CompactionResult contains the result of a compaction operation.
type CompactionResult struct {
	// SegmentsRemoved is the number of segments removed
	SegmentsRemoved int

	// BytesFreed is the total bytes freed
	BytesFreed int64
}

// MetricsSnapshot is a point-in-time view of queue metrics.
type MetricsSnapshot = metrics.Snapshot

// NewMetricsCollector creates a new metrics collector for a queue.
// The queue name is used to identify metrics from this specific queue.
func NewMetricsCollector(queueName string) *metrics.Collector {
	return metrics.NewCollector(queueName)
}

// GetMetricsSnapshot returns a snapshot of current metrics from a collector.
func GetMetricsSnapshot(collector MetricsCollector) *MetricsSnapshot {
	if c, ok := collector.(*metrics.Collector); ok {
		return c.GetSnapshot()
	}
	return nil
}

// DefaultOptions returns sensible defaults for queue configuration.
func DefaultOptions(dir string) *Options {
	return &Options{
		AutoSync:           false,
		SyncInterval:       1 * time.Second,
		CompactionInterval: 0, // Disabled by default
		MaxSegmentSize:     1024 * 1024 * 1024, // 1GB
		MaxSegmentMessages: 0, // Unlimited
		RotationPolicy:     RotateBySize,
		RetentionPolicy:    nil, // No retention
		Logger:             nil, // No logging
		MetricsCollector:   nil, // No metrics
	}
}

// Open opens or creates a queue at the specified directory.
// If opts is nil, default options are used.
func Open(dir string, opts *Options) (*Queue, error) {
	var qopts *queue.Options
	if opts == nil {
		qopts = queue.DefaultOptions(dir)
	} else {
		metricsCollector := opts.MetricsCollector
		if metricsCollector == nil {
			metricsCollector = metrics.NoopCollector{}
		}

		qopts = &queue.Options{
			SegmentOptions:     convertSegmentOptions(dir, opts),
			AutoSync:           opts.AutoSync,
			SyncInterval:       opts.SyncInterval,
			CompactionInterval: opts.CompactionInterval,
			Logger:             convertLogger(opts.Logger),
			MetricsCollector:   metricsCollector,
		}
	}

	q, err := queue.Open(dir, qopts)
	if err != nil {
		return nil, err
	}

	return &Queue{q: q}, nil
}

// Enqueue appends a message to the queue.
// Returns the offset where the message was written.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	return q.q.Enqueue(payload)
}

// EnqueueBatch appends multiple messages to the queue in a single operation.
// This is more efficient than calling Enqueue() multiple times.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatch(payloads [][]byte) ([]uint64, error) {
	return q.q.EnqueueBatch(payloads)
}

// Dequeue retrieves the next message from the queue.
// Returns an error if no messages are available.
func (q *Queue) Dequeue() (*Message, error) {
	msg, err := q.q.Dequeue()
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:        msg.ID,
		Offset:    msg.Offset,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
	}, nil
}

// DequeueBatch retrieves up to maxMessages from the queue in a single operation.
// Returns fewer messages if the queue has fewer than maxMessages available.
func (q *Queue) DequeueBatch(maxMessages int) ([]*Message, error) {
	msgs, err := q.q.DequeueBatch(maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*Message, len(msgs))
	for i, msg := range msgs {
		result[i] = &Message{
			ID:        msg.ID,
			Offset:    msg.Offset,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
		}
	}

	return result, nil
}

// SeekToMessageID sets the read position to a specific message ID.
// Subsequent Dequeue() calls will start reading from this message.
func (q *Queue) SeekToMessageID(msgID uint64) error {
	return q.q.SeekToMessageID(msgID)
}

// SeekToTimestamp sets the read position to the first message at or after the given timestamp.
// The timestamp should be in Unix nanoseconds.
func (q *Queue) SeekToTimestamp(timestamp int64) error {
	return q.q.SeekToTimestamp(timestamp)
}

// Sync forces a sync of pending writes to disk.
func (q *Queue) Sync() error {
	return q.q.Sync()
}

// Close closes the queue and releases all resources.
func (q *Queue) Close() error {
	return q.q.Close()
}

// Stats returns current queue statistics.
func (q *Queue) Stats() *Stats {
	stats := q.q.Stats()
	return &Stats{
		TotalMessages:   stats.TotalMessages,
		PendingMessages: stats.PendingMessages,
		NextMessageID:   stats.NextMessageID,
		ReadMessageID:   stats.ReadMessageID,
		SegmentCount:    stats.SegmentCount,
	}
}

// Compact manually triggers compaction of old segments based on retention policy.
// Returns the number of segments removed and bytes freed.
func (q *Queue) Compact() (*CompactionResult, error) {
	result, err := q.q.Compact()
	if err != nil {
		return nil, err
	}

	return &CompactionResult{
		SegmentsRemoved: result.SegmentsRemoved,
		BytesFreed:      result.BytesFreed,
	}, nil
}

// StreamHandler is called for each message in the stream.
// Return an error to stop streaming.
type StreamHandler func(*Message) error

// Stream continuously reads messages from the queue and calls the handler for each message.
// Streaming continues until the context is cancelled, an error occurs, or no more messages are available.
//
// The Stream method polls for new messages with a configurable interval (100ms by default).
// When a message is available, it's immediately passed to the handler.
// If no messages are available, Stream waits briefly before checking again.
//
// Context cancellation will gracefully stop streaming and return context.Canceled.
// Handler errors will stop streaming and return the handler error.
//
// Example usage:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	err := q.Stream(ctx, func(msg *Message) error {
//	    fmt.Printf("Received: %s\n", msg.Payload)
//	    return nil
//	})
func (q *Queue) Stream(ctx context.Context, handler StreamHandler) error {
	// Convert public handler to internal handler
	internalHandler := func(msg *queue.Message) error {
		return handler(&Message{
			ID:        msg.ID,
			Offset:    msg.Offset,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
		})
	}

	return q.q.Stream(ctx, internalHandler)
}

// Helper functions to convert between public and internal types

func convertSegmentOptions(dir string, opts *Options) *segment.ManagerOptions {
	segOpts := segment.DefaultManagerOptions(dir)

	if opts.MaxSegmentSize > 0 {
		segOpts.MaxSegmentSize = opts.MaxSegmentSize
	}

	if opts.MaxSegmentMessages > 0 {
		segOpts.MaxSegmentMessages = opts.MaxSegmentMessages
	}

	switch opts.RotationPolicy {
	case RotateBySize:
		segOpts.RotationPolicy = segment.RotateBySize
	case RotateByCount:
		segOpts.RotationPolicy = segment.RotateByCount
	case RotateByBoth:
		segOpts.RotationPolicy = segment.RotateByBoth
	}

	if opts.RetentionPolicy != nil {
		segOpts.RetentionPolicy = &segment.RetentionPolicy{
			MaxAge:      opts.RetentionPolicy.MaxAge,
			MaxSize:     opts.RetentionPolicy.MaxSize,
			MaxSegments: opts.RetentionPolicy.MaxSegments,
			MinSegments: opts.RetentionPolicy.MinSegments,
		}
	}

	return segOpts
}

func convertLogger(l Logger) logging.Logger {
	if l == nil {
		return logging.NoopLogger{}
	}
	return &loggerAdapter{l: l}
}

// loggerAdapter adapts public Logger to internal logging.Logger
type loggerAdapter struct {
	l Logger
}

func (a *loggerAdapter) Debug(msg string, fields ...logging.Field) {
	a.l.Debug(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Info(msg string, fields ...logging.Field) {
	a.l.Info(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Warn(msg string, fields ...logging.Field) {
	a.l.Warn(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Error(msg string, fields ...logging.Field) {
	a.l.Error(msg, convertFields(fields)...)
}

func convertFields(fields []logging.Field) []LogField {
	result := make([]LogField, len(fields))
	for i, f := range fields {
		result[i] = LogField{Key: f.Key, Value: f.Value}
	}
	return result
}
