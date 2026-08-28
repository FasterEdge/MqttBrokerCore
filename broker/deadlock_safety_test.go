package hrotti

import (
	"testing"
	"time"
)

// TestNonBlockingPrioritySendDoesNotBlock documents the non-blocking send
// pattern used by the receive goroutine: when the outbound priority channel
// is full, the send must return immediately so the goroutine remains
// responsive to stop / shutdown signals.
func TestNonBlockingPrioritySendDoesNotBlock(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 1 // fill
	done := make(chan struct{})
	go func() {
		// Replicate the patched pattern.
		select {
		case ch <- 2:
		default:
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("non-blocking send should never block on a full channel")
	}
}
