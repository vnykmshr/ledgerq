package queue

import (
	"testing"
)

// TestEnqueueWithHeaders tests basic headers functionality
func TestEnqueueWithHeaders(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue with headers
	headers := map[string]string{
		"content-type":   "application/json",
		"correlation-id": "12345",
		"priority":       "high",
	}
	payload := []byte("test message with headers")
	offset, err := q.EnqueueWithHeaders(payload, headers)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithHeaders() offset = 0, want > 0")
	}

	// Dequeue and verify headers
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if string(msg.Payload) != string(payload) {
		t.Errorf("Payload = %s, want %s", msg.Payload, payload)
	}

	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count = %d, want %d", len(msg.Headers), len(headers))
	}

	for k, v := range headers {
		if msg.Headers[k] != v {
			t.Errorf("Header[%s] = %s, want %s", k, msg.Headers[k], v)
		}
	}
}

// TestEnqueueWithOptions tests combined TTL and headers
func TestEnqueueWithOptions(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue with both TTL and headers
	headers := map[string]string{
		"type":   "event",
		"source": "service-a",
	}
	payload := []byte("message with TTL and headers")
	offset, err := q.EnqueueWithOptions(payload, 5*1000*1000*1000, headers) // 5 seconds
	if err != nil {
		t.Fatalf("EnqueueWithOptions() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithOptions() offset = 0, want > 0")
	}

	// Dequeue immediately (before expiration)
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if string(msg.Payload) != string(payload) {
		t.Errorf("Payload = %s, want %s", msg.Payload, payload)
	}

	// Verify headers
	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count = %d, want %d", len(msg.Headers), len(headers))
	}

	for k, v := range headers {
		if msg.Headers[k] != v {
			t.Errorf("Header[%s] = %s, want %s", k, msg.Headers[k], v)
		}
	}

	// Verify TTL is set
	if msg.ExpiresAt == 0 {
		t.Error("ExpiresAt = 0, want > 0 for TTL message")
	}
}

// TestHeaders_EmptyMap tests that empty headers work correctly
func TestHeaders_EmptyMap(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue with empty headers map
	headers := map[string]string{}
	payload := []byte("message with empty headers")
	offset, err := q.EnqueueWithHeaders(payload, headers)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithHeaders() offset = 0, want > 0")
	}

	// Dequeue and verify no headers
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	// Empty headers map should not set the flag, so Headers should be nil
	if msg.Headers != nil && len(msg.Headers) > 0 {
		t.Errorf("Headers = %v, want nil or empty", msg.Headers)
	}
}

// TestHeaders_NilMap tests that nil headers work correctly
func TestHeaders_NilMap(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue with nil headers
	payload := []byte("message with nil headers")
	offset, err := q.EnqueueWithHeaders(payload, nil)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithHeaders() offset = 0, want > 0")
	}

	// Dequeue and verify no headers
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if msg.Headers != nil && len(msg.Headers) > 0 {
		t.Errorf("Headers = %v, want nil or empty", msg.Headers)
	}
}

// TestHeaders_LargeHeaders tests headers with large values
func TestHeaders_LargeHeaders(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Create headers with large values
	largeValue := string(make([]byte, 1000)) // 1KB value
	headers := map[string]string{
		"large-header": largeValue,
		"key1":         "value1",
		"key2":         "value2",
	}

	payload := []byte("message with large headers")
	offset, err := q.EnqueueWithHeaders(payload, headers)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithHeaders() offset = 0, want > 0")
	}

	// Dequeue and verify
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count = %d, want %d", len(msg.Headers), len(headers))
	}

	if msg.Headers["large-header"] != largeValue {
		t.Error("Large header value mismatch")
	}
}

// TestHeaders_ManyHeaders tests a message with many headers
func TestHeaders_ManyHeaders(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Create many headers
	headers := make(map[string]string)
	for i := 0; i < 50; i++ {
		headers[string(rune('a'+i%26))+string(rune('0'+i/26))] = string(rune('A' + i%26))
	}

	payload := []byte("message with many headers")
	offset, err := q.EnqueueWithHeaders(payload, headers)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if offset == 0 {
		t.Error("EnqueueWithHeaders() offset = 0, want > 0")
	}

	// Dequeue and verify
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count = %d, want %d", len(msg.Headers), len(headers))
	}

	for k, v := range headers {
		if msg.Headers[k] != v {
			t.Errorf("Header[%s] = %s, want %s", k, msg.Headers[k], v)
		}
	}
}

// TestHeaders_Persistence tests that headers persist across queue restarts
func TestHeaders_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: enqueue with headers
	q1, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() first error = %v", err)
	}

	headers := map[string]string{
		"session": "first",
		"data":    "persisted",
	}
	_, err = q1.EnqueueWithHeaders([]byte("persistent message"), headers)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	if err := q1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Second session: verify headers are preserved
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	msg, err := q2.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if len(msg.Headers) != len(headers) {
		t.Errorf("Headers count after persistence = %d, want %d", len(msg.Headers), len(headers))
	}

	for k, v := range headers {
		if msg.Headers[k] != v {
			t.Errorf("Header[%s] after persistence = %s, want %s", k, msg.Headers[k], v)
		}
	}
}

// TestHeaders_BatchDequeue tests headers with batch dequeue
func TestHeaders_BatchDequeue(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue multiple messages with different headers
	for i := 0; i < 5; i++ {
		headers := map[string]string{
			"index":  string(rune('0' + i)),
			"common": "value",
		}
		payload := []byte("message " + string(rune('0'+i)))
		_, err = q.EnqueueWithHeaders(payload, headers)
		if err != nil {
			t.Fatalf("EnqueueWithHeaders() %d error = %v", i, err)
		}
	}

	// Batch dequeue
	messages, err := q.DequeueBatch(10)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 5 {
		t.Fatalf("Got %d messages, want 5", len(messages))
	}

	// Verify each message has its headers
	for i, msg := range messages {
		if len(msg.Headers) != 2 {
			t.Errorf("Message %d headers count = %d, want 2", i, len(msg.Headers))
		}

		if msg.Headers["index"] != string(rune('0'+i)) {
			t.Errorf("Message %d index header = %s, want %c", i, msg.Headers["index"], '0'+i)
		}

		if msg.Headers["common"] != "value" {
			t.Errorf("Message %d common header = %s, want value", i, msg.Headers["common"])
		}
	}
}

// TestHeaders_MixedMessages tests mix of messages with and without headers
func TestHeaders_MixedMessages(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue different types of messages
	// 1. Normal message
	_, err = q.Enqueue([]byte("normal"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// 2. Message with headers
	headers1 := map[string]string{"type": "with-headers"}
	_, err = q.EnqueueWithHeaders([]byte("with headers"), headers1)
	if err != nil {
		t.Fatalf("EnqueueWithHeaders() error = %v", err)
	}

	// 3. Message with TTL
	_, err = q.EnqueueWithTTL([]byte("with ttl"), 5*1000*1000*1000)
	if err != nil {
		t.Fatalf("EnqueueWithTTL() error = %v", err)
	}

	// 4. Message with both TTL and headers
	headers2 := map[string]string{"type": "both"}
	_, err = q.EnqueueWithOptions([]byte("with both"), 5*1000*1000*1000, headers2)
	if err != nil {
		t.Fatalf("EnqueueWithOptions() error = %v", err)
	}

	// Dequeue all and verify
	// Message 1: Normal
	msg1, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() 1 error = %v", err)
	}
	if string(msg1.Payload) != "normal" {
		t.Errorf("Message 1 payload = %s, want normal", msg1.Payload)
	}
	if msg1.Headers != nil && len(msg1.Headers) > 0 {
		t.Errorf("Message 1 should have no headers, got %v", msg1.Headers)
	}
	if msg1.ExpiresAt != 0 {
		t.Errorf("Message 1 should have no TTL")
	}

	// Message 2: With headers
	msg2, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() 2 error = %v", err)
	}
	if string(msg2.Payload) != "with headers" {
		t.Errorf("Message 2 payload = %s, want with headers", msg2.Payload)
	}
	if msg2.Headers["type"] != "with-headers" {
		t.Errorf("Message 2 headers incorrect")
	}
	if msg2.ExpiresAt != 0 {
		t.Errorf("Message 2 should have no TTL")
	}

	// Message 3: With TTL
	msg3, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() 3 error = %v", err)
	}
	if string(msg3.Payload) != "with ttl" {
		t.Errorf("Message 3 payload = %s, want with ttl", msg3.Payload)
	}
	if msg3.Headers != nil && len(msg3.Headers) > 0 {
		t.Errorf("Message 3 should have no headers, got %v", msg3.Headers)
	}
	if msg3.ExpiresAt == 0 {
		t.Errorf("Message 3 should have TTL")
	}

	// Message 4: With both
	msg4, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() 4 error = %v", err)
	}
	if string(msg4.Payload) != "with both" {
		t.Errorf("Message 4 payload = %s, want with both", msg4.Payload)
	}
	if msg4.Headers["type"] != "both" {
		t.Errorf("Message 4 headers incorrect")
	}
	if msg4.ExpiresAt == 0 {
		t.Errorf("Message 4 should have TTL")
	}
}
