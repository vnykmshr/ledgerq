package ledgerq

import "time"

// Option is a functional option for configuring a Queue.
type Option func(*Queue) error

// Options contains configuration parameters for a Queue.
type Options struct {
	// MaxSegmentSize is the maximum size of a segment file in bytes.
	// Default: 64 MB
	MaxSegmentSize int64

	// SyncInterval specifies how often to sync data to disk.
	// Set to 0 for synchronous writes (fsync after every write).
	// Default: 1 second
	SyncInterval time.Duration

	// AutoCompact enables automatic compaction of acknowledged segments.
	// Default: true
	AutoCompact bool

	// CompactInterval specifies how often to run compaction.
	// Default: 5 minutes
	CompactInterval time.Duration

	// ReadOnly opens the queue in read-only mode.
	// Default: false
	ReadOnly bool
}

// DefaultOptions returns the default configuration options.
func DefaultOptions() Options {
	return Options{
		MaxSegmentSize:  64 * 1024 * 1024, // 64 MB
		SyncInterval:    time.Second,
		AutoCompact:     true,
		CompactInterval: 5 * time.Minute,
		ReadOnly:        false,
	}
}

// WithMaxSegmentSize sets the maximum segment file size.
func WithMaxSegmentSize(_ int64) Option {
	return func(_ *Queue) error {
		// Implementation will be added in Phase 3
		return nil
	}
}

// WithSyncInterval sets the fsync interval.
func WithSyncInterval(_ time.Duration) Option {
	return func(_ *Queue) error {
		// Implementation will be added in Phase 3
		return nil
	}
}

// WithAutoCompact enables or disables automatic compaction.
func WithAutoCompact(_ bool) Option {
	return func(_ *Queue) error {
		// Implementation will be added in Phase 3
		return nil
	}
}

// WithReadOnly opens the queue in read-only mode.
func WithReadOnly() Option {
	return func(_ *Queue) error {
		// Implementation will be added in Phase 3
		return nil
	}
}
