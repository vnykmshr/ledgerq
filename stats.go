package ledgerq

// Stats contains metrics and statistics about the queue.
type Stats struct {
	// TotalMessages is the total number of messages ever enqueued.
	TotalMessages int64

	// PendingMessages is the number of messages awaiting dequeue.
	PendingMessages int64

	// AckedMessages is the number of acknowledged messages.
	AckedMessages int64

	// SegmentCount is the number of segment files.
	SegmentCount int

	// DiskUsageBytes is the total disk space used by the queue.
	DiskUsageBytes int64

	// OldestOffset is the offset of the oldest message.
	OldestOffset uint64

	// NewestOffset is the offset of the newest message.
	NewestOffset uint64
}

// Stats returns current queue statistics.
func (q *Queue) Stats() Stats {
	// Implementation will be added in Phase 7
	return Stats{}
}
