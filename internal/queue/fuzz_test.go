package queue

import (
	"bytes"
	"testing"
)

// FuzzEnqueueDequeue tests enqueue/dequeue with fuzz inputs
func FuzzEnqueueDequeue(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("hello world"))
	f.Add([]byte(""))
	f.Add([]byte("a"))
	f.Add(make([]byte, 100))
	f.Add(make([]byte, 1000))

	f.Fuzz(func(t *testing.T, payload []byte) {
		// Skip very large payloads to avoid OOM
		if len(payload) > 1*1024*1024 {
			t.Skip()
		}

		tmpDir := t.TempDir()

		// Open queue
		q, err := Open(tmpDir, nil)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer q.Close()

		// Enqueue
		offset, err := q.Enqueue(payload)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}

		if offset == 0 {
			t.Error("Expected non-zero offset")
		}

		// Dequeue
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}

		// Verify payload matches
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("Payload mismatch: got %d bytes, want %d bytes", len(msg.Payload), len(payload))
		}

		// Verify message ID
		if msg.ID != 1 {
			t.Errorf("Expected message ID 1, got %d", msg.ID)
		}
	})
}

// FuzzEnqueueBatch tests batch enqueue with fuzz inputs
func FuzzEnqueueBatch(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("msg1"), []byte("msg2"), []byte("msg3"))
	f.Add([]byte(""), []byte("a"), []byte("test"))
	f.Add(make([]byte, 10), make([]byte, 20), make([]byte, 30))

	f.Fuzz(func(t *testing.T, p1, p2, p3 []byte) {
		// Skip very large payloads
		if len(p1)+len(p2)+len(p3) > 1*1024*1024 {
			t.Skip()
		}

		tmpDir := t.TempDir()

		q, err := Open(tmpDir, nil)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer q.Close()

		// Enqueue batch
		payloads := [][]byte{p1, p2, p3}
		offsets, err := q.EnqueueBatch(payloads)
		if err != nil {
			t.Fatalf("EnqueueBatch failed: %v", err)
		}

		if len(offsets) != 3 {
			t.Errorf("Expected 3 offsets, got %d", len(offsets))
		}

		// Dequeue and verify
		for i, expected := range payloads {
			msg, err := q.Dequeue()
			if err != nil {
				t.Fatalf("Dequeue %d failed: %v", i, err)
			}

			if !bytes.Equal(msg.Payload, expected) {
				t.Errorf("Message %d: payload mismatch", i)
			}
		}
	})
}

// FuzzSeekToMessageID tests seeking with fuzz inputs
func FuzzSeekToMessageID(f *testing.F) {
	f.Add(uint64(1))
	f.Add(uint64(0))
	f.Add(uint64(100))
	f.Add(uint64(^uint64(0))) // max uint64

	f.Fuzz(func(t *testing.T, seekID uint64) {
		tmpDir := t.TempDir()

		q, err := Open(tmpDir, nil)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer q.Close()

		// Enqueue some messages
		for i := 0; i < 10; i++ {
			if _, err := q.Enqueue([]byte("test")); err != nil {
				t.Fatalf("Enqueue failed: %v", err)
			}
		}

		// Try to seek
		err = q.SeekToMessageID(seekID)

		// Seek should fail for invalid IDs
		if seekID == 0 || seekID >= 11 {
			if err == nil {
				t.Error("Expected seek to fail for invalid ID")
			}
		} else {
			// Seek should succeed for valid IDs (1-10)
			if err != nil {
				t.Errorf("Seek to valid ID %d failed: %v", seekID, err)
			} else {
				// Verify we can dequeue from that position
				msg, err := q.Dequeue()
				if err != nil {
					t.Errorf("Dequeue after seek failed: %v", err)
				} else if msg.ID != seekID {
					t.Errorf("After seek to %d, got message ID %d", seekID, msg.ID)
				}
			}
		}
	})
}

// FuzzQueuePersistence tests queue persistence with fuzz inputs
func FuzzQueuePersistence(f *testing.F) {
	f.Add([]byte("test1"), []byte("test2"))
	f.Add([]byte(""), []byte("data"))
	f.Add(make([]byte, 50), make([]byte, 100))

	f.Fuzz(func(t *testing.T, p1, p2 []byte) {
		if len(p1)+len(p2) > 1*1024*1024 {
			t.Skip()
		}

		tmpDir := t.TempDir()

		// First session: write
		q1, err := Open(tmpDir, nil)
		if err != nil {
			t.Fatalf("Open first session failed: %v", err)
		}

		if _, err := q1.Enqueue(p1); err != nil {
			t.Fatalf("Enqueue p1 failed: %v", err)
		}
		if _, err := q1.Enqueue(p2); err != nil {
			t.Fatalf("Enqueue p2 failed: %v", err)
		}

		// Consume first message
		msg1, err := q1.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue first session failed: %v", err)
		}
		if !bytes.Equal(msg1.Payload, p1) {
			t.Error("First message mismatch")
		}

		if err := q1.Close(); err != nil {
			t.Fatalf("Close first session failed: %v", err)
		}

		// Second session: read
		q2, err := Open(tmpDir, nil)
		if err != nil {
			t.Fatalf("Open second session failed: %v", err)
		}
		defer q2.Close()

		// Should read second message (read position persisted)
		msg2, err := q2.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue second session failed: %v", err)
		}
		if !bytes.Equal(msg2.Payload, p2) {
			t.Error("Second message mismatch after reopen")
		}

		// Should be empty
		_, err = q2.Dequeue()
		if err == nil {
			t.Error("Expected no more messages")
		}
	})
}

// FuzzMetadataOperations tests metadata operations with fuzz inputs
func FuzzMetadataOperations(f *testing.F) {
	f.Add(uint64(1), uint64(1))
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(100), uint64(50))
	f.Add(uint64(^uint64(0)), uint64(^uint64(0)))

	f.Fuzz(func(t *testing.T, nextID, readID uint64) {
		tmpDir := t.TempDir()

		m, err := OpenMetadata(tmpDir, false)
		if err != nil {
			t.Fatalf("OpenMetadata failed: %v", err)
		}
		defer m.Close()

		// Try to update state
		// Note: metadata allows any uint64 values (no validation)
		if err := m.UpdateState(nextID, readID); err != nil {
			t.Fatalf("UpdateState failed: %v", err)
		}

		// Verify state
		gotNext, gotRead := m.GetState()
		if gotNext != nextID {
			t.Errorf("NextMsgID: got %d, want %d", gotNext, nextID)
		}
		if gotRead != readID {
			t.Errorf("ReadMsgID: got %d, want %d", gotRead, readID)
		}

		// Close and reopen to test persistence
		if err := m.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		m2, err := OpenMetadata(tmpDir, false)
		if err != nil {
			t.Fatalf("Reopen metadata failed: %v", err)
		}
		defer m2.Close()

		// Verify persistence
		gotNext2, gotRead2 := m2.GetState()
		if gotNext2 != nextID {
			t.Errorf("After reopen, NextMsgID: got %d, want %d", gotNext2, nextID)
		}
		if gotRead2 != readID {
			t.Errorf("After reopen, ReadMsgID: got %d, want %d", gotRead2, readID)
		}
	})
}
