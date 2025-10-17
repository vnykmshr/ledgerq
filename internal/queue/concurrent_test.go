package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentEnqueue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Concurrent enqueuers
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < messagesPerGoroutine; i++ {
				payload := []byte(fmt.Sprintf("goroutine-%d-msg-%d", id, i))
				if _, err := q.Enqueue(payload); err != nil {
					t.Errorf("Enqueue() error = %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all messages were written
	stats := q.Stats()
	expected := uint64(numGoroutines * messagesPerGoroutine)
	if stats.TotalMessages != expected {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, expected)
	}
}

func TestConcurrentDequeue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages first
	totalMessages := 1000
	for i := 0; i < totalMessages; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Concurrent dequeuers
	numGoroutines := 10
	messagesChan := make(chan *Message, totalMessages)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			for {
				msg, err := q.Dequeue()
				if err != nil {
					// No more messages
					return
				}
				messagesChan <- msg
			}
		}()
	}

	wg.Wait()
	close(messagesChan)

	// Verify we got all messages
	messagesReceived := 0
	for range messagesChan {
		messagesReceived++
	}

	if messagesReceived != totalMessages {
		t.Errorf("Received %d messages, want %d", messagesReceived, totalMessages)
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Concurrent producers and consumers
	numProducers := 5
	numConsumers := 5
	messagesPerProducer := 100

	var wg sync.WaitGroup

	// Start producers
	wg.Add(numProducers)
	for p := 0; p < numProducers; p++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < messagesPerProducer; i++ {
				payload := []byte(fmt.Sprintf("producer-%d-msg-%d", id, i))
				if _, err := q.Enqueue(payload); err != nil {
					t.Errorf("Enqueue() error = %v", err)
					return
				}
			}
		}(p)
	}

	// Give producers a head start
	time.Sleep(100 * time.Millisecond)

	// Start consumers
	consumed := make(chan int, numConsumers)
	wg.Add(numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			count := 0
			for {
				_, err := q.Dequeue()
				if err != nil {
					// No more messages available
					time.Sleep(10 * time.Millisecond)
					// Try one more time
					_, err := q.Dequeue()
					if err != nil {
						consumed <- count
						return
					}
					count++
				} else {
					count++
				}
			}
		}()
	}

	wg.Wait()
	close(consumed)

	// Count total consumed
	totalConsumed := 0
	for c := range consumed {
		totalConsumed += c
	}

	expectedTotal := numProducers * messagesPerProducer
	if totalConsumed != expectedTotal {
		t.Errorf("Consumed %d messages, want %d", totalConsumed, expectedTotal)
	}
}

func TestConcurrentBatchOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Concurrent batch enqueuers
	numGoroutines := 10
	batchesPerGoroutine := 10
	messagesPerBatch := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for b := 0; b < batchesPerGoroutine; b++ {
				batch := make([][]byte, messagesPerBatch)
				for i := 0; i < messagesPerBatch; i++ {
					batch[i] = []byte(fmt.Sprintf("g%d-b%d-m%d", id, b, i))
				}
				if _, err := q.EnqueueBatch(batch); err != nil {
					t.Errorf("EnqueueBatch() error = %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify total messages
	stats := q.Stats()
	expected := uint64(numGoroutines * batchesPerGoroutine * messagesPerBatch)
	if stats.TotalMessages != expected {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, expected)
	}
}

func TestConcurrentSeek(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages first
	totalMessages := 1000
	for i := 0; i < totalMessages; i++ {
		if _, err := q.Enqueue([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}

	// Concurrent seekers and readers
	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			// Each goroutine seeks to different positions
			seekPos := uint64(1 + (id * 100))
			if err := q.SeekToMessageID(seekPos); err != nil {
				t.Errorf("SeekToMessageID(%d) error = %v", seekPos, err)
				return
			}

			// Read a few messages
			for i := 0; i < 5; i++ {
				if _, err := q.Dequeue(); err != nil {
					// It's ok if we can't read (another goroutine might have consumed)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestConcurrentMixedOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Run mixed operations concurrently
	var wg sync.WaitGroup

	// Enqueuers
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = q.Enqueue([]byte(fmt.Sprintf("enq-%d-%d", id, j)))
			}
		}(i)
	}

	// Batch enqueuers
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				batch := make([][]byte, 10)
				for k := 0; k < 10; k++ {
					batch[k] = []byte(fmt.Sprintf("batch-%d-%d-%d", id, j, k))
				}
				_, _ = q.EnqueueBatch(batch)
			}
		}(i)
	}

	// Give enqueuers a head start
	time.Sleep(100 * time.Millisecond)

	// Dequeuers
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = q.Dequeue()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Seekers
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = q.SeekToMessageID(1)
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	// Stats readers
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = q.Stats()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// If we get here without deadlock or panic, test passes
}

func TestConcurrentSync(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	var wg sync.WaitGroup

	// Writers
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = q.Enqueue([]byte(fmt.Sprintf("msg-%d-%d", id, j)))
			}
		}(i)
	}

	// Concurrent syncers
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = q.Sync()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}
