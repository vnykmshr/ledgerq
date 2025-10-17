package ledgerq

import "errors"

// Common errors returned by LedgerQ operations.
var (
	// ErrNotImplemented indicates the feature is not yet implemented.
	ErrNotImplemented = errors.New("ledgerq: not implemented")

	// ErrQueueClosed indicates the queue has been closed.
	ErrQueueClosed = errors.New("ledgerq: queue closed")

	// ErrQueueEmpty indicates there are no messages available.
	ErrQueueEmpty = errors.New("ledgerq: queue empty")

	// ErrInvalidOffset indicates the requested offset is invalid.
	ErrInvalidOffset = errors.New("ledgerq: invalid offset")

	// ErrCorrupted indicates the queue data is corrupted.
	ErrCorrupted = errors.New("ledgerq: data corrupted")

	// ErrReadOnly indicates a write operation on a read-only queue.
	ErrReadOnly = errors.New("ledgerq: queue is read-only")

	// ErrInvalidMessage indicates the message format is invalid.
	ErrInvalidMessage = errors.New("ledgerq: invalid message")
)
