// Package ledgerq provides a lightweight, durable, file-backed queue for Go.
//
// LedgerQ is designed for embedded and local-first applications that need
// reliable message persistence without external dependencies. It guarantees
// crash-safe durability, supports batch operations, and provides flexible
// replay capabilities.
//
// Example usage:
//
//	q, err := ledgerq.Open("./myqueue")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Enqueue a message
//	if err := q.Enqueue([]byte("hello")); err != nil {
//		log.Fatal(err)
//	}
//
//	// Dequeue a message
//	msg, err := q.Dequeue()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Acknowledge the message
//	if err := q.Ack(msg.ID); err != nil {
//		log.Fatal(err)
//	}
package ledgerq

// Queue represents a durable, file-backed message queue.
// It provides crash-safe persistence with replay capabilities.
type Queue struct {
	// Implementation will be added in Phase 3
}

// Open creates or opens an existing queue at the specified directory.
// If the directory doesn't exist, it will be created.
func Open(_ string, _ ...Option) (*Queue, error) {
	// Implementation will be added in Phase 3
	return nil, ErrNotImplemented
}

// Close gracefully shuts down the queue, ensuring all in-flight
// operations are persisted to disk.
func (q *Queue) Close() error {
	// Implementation will be added in Phase 3
	return ErrNotImplemented
}

// Enqueue adds a single message to the queue.
func (q *Queue) Enqueue(_ []byte) error {
	// Implementation will be added in Phase 3
	return ErrNotImplemented
}

// Dequeue retrieves the next message from the queue.
// The message remains in the queue until acknowledged.
func (q *Queue) Dequeue() (*Message, error) {
	// Implementation will be added in Phase 3
	return nil, ErrNotImplemented
}

// Ack acknowledges a message, marking it for removal during compaction.
func (q *Queue) Ack(_ uint64) error {
	// Implementation will be added in Phase 3
	return ErrNotImplemented
}
