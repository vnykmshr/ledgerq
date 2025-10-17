package ledgerq

import "time"

// Message represents a single message in the queue.
type Message struct {
	// ID is the unique identifier for this message.
	ID uint64

	// Offset is the position of this message in the queue.
	Offset uint64

	// Data is the message payload.
	Data []byte

	// Timestamp is when the message was enqueued.
	Timestamp time.Time
}
