package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentEnqueue(t *testing.T) {
	q := setupQueue(t, nil)

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
	expected := uint64(numGoroutines * messagesPerGoroutine)
	assertStats(t, q, expected, expected)
}

func TestConcurrentDequeue(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue messages first
	totalMessages := 1000
	enqueueN(t, q, totalMessages)

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
	q := setupQueue(t, nil)

	// Concurrent producers and consumers
	numProducers := 5
	numConsumers := 5
	messagesPerProducer := 100
	expectedTotal := numProducers * messagesPerProducer

	var wg sync.WaitGroup
	producerReady := make(chan struct{})

	// Start producers
	wg.Add(numProducers)
	for p := 0; p < numProducers; p++ {
		go func(id int) {
			defer wg.Done()
			if id == 0 {
				close(producerReady) // Signal that at least one producer is ready
			}
			for i := 0; i < messagesPerProducer; i++ {
				payload := []byte(fmt.Sprintf("producer-%d-msg-%d", id, i))
				if _, err := q.Enqueue(payload); err != nil {
					t.Errorf("Enqueue() error = %v", err)
					return
				}
			}
		}(p)
	}

	// Wait for producers to start, then start consumers
	<-producerReady

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
					// Check if all messages have been produced
					stats := q.Stats()
					if stats.TotalMessages >= uint64(expectedTotal) && stats.PendingMessages == 0 {
						consumed <- count
						return
					}
					// Otherwise wait briefly for more messages
					time.Sleep(10 * time.Millisecond)
					continue
				}
				count++
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

	if totalConsumed != expectedTotal {
		t.Errorf("Consumed %d messages, want %d", totalConsumed, expectedTotal)
	}
}

func TestConcurrentBatchOperations(t *testing.T) {
	q := setupQueue(t, nil)

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
	expected := uint64(numGoroutines * batchesPerGoroutine * messagesPerBatch)
	assertStats(t, q, expected, expected)
}

func TestConcurrentSeek(t *testing.T) {
	q := setupQueue(t, nil)

	// Enqueue messages first
	totalMessages := 1000
	enqueueN(t, q, totalMessages)

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
	q := setupQueue(t, nil)

	// Run mixed operations concurrently
	var wg sync.WaitGroup
	ready := make(chan struct{})

	// Enqueuers
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			defer wg.Done()
			if id == 0 {
				close(ready) // Signal ready
			}
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

	// Wait for enqueuers to start
	<-ready

	// Dequeuers
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = q.Dequeue()
				time.Sleep(time.Millisecond) // Rate limiting, not synchronization
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
				time.Sleep(10 * time.Millisecond) // Rate limiting, not synchronization
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
				time.Sleep(time.Millisecond) // Rate limiting, not synchronization
			}
		}()
	}

	wg.Wait()

	// If we get here without deadlock or panic, test passes
}

func TestConcurrentSync(t *testing.T) {
	q := setupQueue(t, nil)

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
				time.Sleep(5 * time.Millisecond) // Rate limiting, not synchronization
			}
		}()
	}

	wg.Wait()
}
