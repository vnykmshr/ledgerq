// Package queue provides streaming API for real-time message delivery.
// This file contains the streaming functionality for continuous message processing.
package queue

import (
	"context"
	"fmt"
	"time"
)

// StreamHandler is called for each message in the stream.
// Return an error to stop streaming.
type StreamHandler func(*Message) error

// Stream continuously reads messages from the queue and calls the handler for each message.
// Streaming continues until the context is cancelled, an error occurs, or no more messages are available.
// The handler is called for each message in order.
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
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// Poll interval for checking new messages
	pollInterval := 100 * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled - graceful shutdown
			return ctx.Err()

		case <-ticker.C:
			// Try to dequeue a message
			msg, err := q.Dequeue()
			if err != nil {
				// Check if error is "no messages available"
				// If so, continue polling; otherwise return error
				if err.Error() == "no messages available" {
					continue
				}
				return fmt.Errorf("stream dequeue error: %w", err)
			}

			// Call handler with the message
			if err := handler(msg); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}

			// Reset ticker to immediately check for next message
			// This provides better throughput when messages are available
			ticker.Reset(pollInterval)
		}
	}
}
