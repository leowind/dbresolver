package dbresolver

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestPolicy_RoundRobinPolicy(t *testing.T) {
	var p1, p2, p3 gorm.ConnPool
	var pools = []gorm.ConnPool{
		p1, p2, p3,
	}

	for i := 0; i < 10; i++ {
		if pools[i%3] != RoundRobinPolicy().Resolve(pools) {
			t.Errorf("RoundRobinPolicy failed")
		}
		if pools[i%3] != StrictRoundRobinPolicy().Resolve(pools) {
			t.Errorf("StrictRoundRobinPolicy failed")
		}
	}
}

func BenchmarkPolicy_StrictRoundRobinPolicy(b *testing.B) {
	var p1, p2, p3 gorm.ConnPool
	var pools = []gorm.ConnPool{
		p1, p2, p3,
	}

	var i int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if pools[int(atomic.AddInt64(&i, 1))%3] != StrictRoundRobinPolicy().Resolve(pools) {
				b.Errorf("RoundRobinPolicy failed")
			}
		}
	})
}

func TestHealthTracker_SingleSuccess(t *testing.T) {
	// Test backward compatible behavior - 1 success should immediately recover
	tracker := NewHealthTracker(100 * time.Millisecond)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance // Use nil ConnPool as identifier (pointer-based key)

	// Mark as bad
	tracker.MarkBad(pool)
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad immediately after MarkBad")
	}

	// Wait for cooldown
	time.Sleep(150 * time.Millisecond)

	// Should now be in half-open state (not bad, can be probed)
	if tracker.IsBad(pool) {
		t.Error("Pool should not be bad after cooldown (half-open state)")
	}

	// Check state
	state := tracker.GetState(pool)
	if state != "probing (0/1)" {
		t.Errorf("Expected state 'probing (0/1)', got '%s'", state)
	}

	// One success should fully recover (successesNeeded = 1)
	tracker.MarkHealthy(pool)

	if tracker.IsBad(pool) {
		t.Error("Pool should be healthy after 1 success")
	}

	state = tracker.GetState(pool)
	if state != "healthy" {
		t.Errorf("Expected state 'healthy', got '%s'", state)
	}
}

func TestHealthTracker_MultipleSuccessesRequired(t *testing.T) {
	// Test requiring 3 consecutive successes
	tracker := NewHealthTrackerWithSuccesses(100*time.Millisecond, 3)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	// Mark as bad
	tracker.MarkBad(pool)
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad immediately after MarkBad")
	}

	// Wait for cooldown
	time.Sleep(150 * time.Millisecond)

	// Should now be in half-open state
	if tracker.IsBad(pool) {
		t.Error("Pool should not be bad after cooldown (half-open state)")
	}

	// First success - should still be probing
	tracker.MarkHealthy(pool)
	state := tracker.GetState(pool)
	if state != "probing (1/3)" {
		t.Errorf("Expected state 'probing (1/3)', got '%s'", state)
	}

	// Second success - should still be probing
	tracker.MarkHealthy(pool)
	state = tracker.GetState(pool)
	if state != "probing (2/3)" {
		t.Errorf("Expected state 'probing (2/3)', got '%s'", state)
	}

	// Third success - should now be fully healthy
	tracker.MarkHealthy(pool)
	state = tracker.GetState(pool)
	if state != "healthy" {
		t.Errorf("Expected state 'healthy', got '%s'", state)
	}

	if tracker.IsBad(pool) {
		t.Error("Pool should be healthy after 3 successes")
	}
}

func TestHealthTracker_FailureResetsCounter(t *testing.T) {
	// Test that failure during probing resets the counter
	tracker := NewHealthTrackerWithSuccesses(100*time.Millisecond, 3)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	// Mark as bad and wait for cooldown
	tracker.MarkBad(pool)
	time.Sleep(150 * time.Millisecond)

	// Two successes
	tracker.MarkHealthy(pool)
	tracker.MarkHealthy(pool)

	state := tracker.GetState(pool)
	if state != "probing (2/3)" {
		t.Errorf("Expected state 'probing (2/3)', got '%s'", state)
	}

	// Failure should reset counter and restart cooldown
	tracker.MarkBad(pool)
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad immediately after MarkBad")
	}

	// Wait for cooldown again
	time.Sleep(150 * time.Millisecond)

	// Counter should be reset to 0
	state = tracker.GetState(pool)
	if state != "probing (0/3)" {
		t.Errorf("Expected state 'probing (0/3)' after reset, got '%s'", state)
	}

	// Should need 3 successes again from scratch
	tracker.MarkHealthy(pool)
	state = tracker.GetState(pool)
	if state != "probing (1/3)" {
		t.Errorf("Expected state 'probing (1/3)', got '%s'", state)
	}
}

func TestHealthTracker_NoProbingDuringCooldown(t *testing.T) {
	// Test that pool is excluded during cooldown period
	tracker := NewHealthTrackerWithSuccesses(200*time.Millisecond, 2)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	// Mark as bad
	tracker.MarkBad(pool)

	// Should be bad immediately
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad during cooldown")
	}

	state := tracker.GetState(pool)
	if state != "bad" {
		t.Errorf("Expected state 'bad', got '%s'", state)
	}

	// Wait 100ms (half of cooldown)
	time.Sleep(100 * time.Millisecond)

	// Should still be bad
	if !tracker.IsBad(pool) {
		t.Error("Pool should still be bad before cooldown expires")
	}

	// Wait for cooldown to expire
	time.Sleep(150 * time.Millisecond)

	// Should now be in half-open state (not bad)
	if tracker.IsBad(pool) {
		t.Error("Pool should not be bad after cooldown expires")
	}

	state = tracker.GetState(pool)
	if state != "probing (0/2)" {
		t.Errorf("Expected state 'probing (0/2)', got '%s'", state)
	}
}

func TestHealthTracker_FlappingPrevention(t *testing.T) {
	// Test that requiring multiple successes prevents flapping
	tracker := NewHealthTrackerWithSuccesses(500*time.Millisecond, 5)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	// Simulate flapping scenario: bad → cooldown → 4 successes → failure
	tracker.MarkBad(pool)
	time.Sleep(550 * time.Millisecond)

	// 4 successes (not enough for recovery)
	for i := 0; i < 4; i++ {
		tracker.MarkHealthy(pool)
	}

	state := tracker.GetState(pool)
	if state != "probing (4/5)" {
		t.Errorf("Expected state 'probing (4/5)', got '%s'", state)
	}

	// Failure before reaching threshold
	tracker.MarkBad(pool)

	// Should be back to bad state
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad after failure during probing")
	}

	state = tracker.GetState(pool)
	if state != "bad" {
		t.Errorf("Expected state 'bad', got '%s'", state)
	}

	// Wait for cooldown
	time.Sleep(550 * time.Millisecond)

	// Counter should be reset
	state = tracker.GetState(pool)
	if state != "probing (0/5)" {
		t.Errorf("Expected state 'probing (0/5)' after reset, got '%s'", state)
	}
}

func TestHealthTracker_MultiplePools(t *testing.T) {
	// Test that multiple pools are tracked independently
	tracker := NewHealthTrackerWithSuccesses(100*time.Millisecond, 2)
	var pool1, pool2 gorm.ConnPool
	pool1 = &struct{ gorm.ConnPool }{} // Different instances for tracking
	pool2 = &struct{ gorm.ConnPool }{}

	// Mark both as bad
	tracker.MarkBad(pool1)
	tracker.MarkBad(pool2)

	time.Sleep(150 * time.Millisecond)

	// One success on pool1
	tracker.MarkHealthy(pool1)

	// Check states are independent
	state1 := tracker.GetState(pool1)
	state2 := tracker.GetState(pool2)

	if state1 != "probing (1/2)" {
		t.Errorf("Pool1: expected 'probing (1/2)', got '%s'", state1)
	}
	if state2 != "probing (0/2)" {
		t.Errorf("Pool2: expected 'probing (0/2)', got '%s'", state2)
	}

	// Recover pool1 fully
	tracker.MarkHealthy(pool1)
	if tracker.GetState(pool1) != "healthy" {
		t.Error("Pool1 should be healthy")
	}

	// Pool2 should still be probing
	if tracker.GetState(pool2) != "probing (0/2)" {
		t.Error("Pool2 should still be probing")
	}
}

func TestHealthTracker_ConcurrentAccess(t *testing.T) {
	// Test thread safety with concurrent operations
	tracker := NewHealthTrackerWithSuccesses(50*time.Millisecond, 3)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent MarkBad calls
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.MarkBad(pool)
		}()
	}

	wg.Wait()

	// Should be bad
	if !tracker.IsBad(pool) {
		t.Error("Pool should be bad after concurrent MarkBad calls")
	}

	// Wait for cooldown
	time.Sleep(100 * time.Millisecond)

	// Concurrent MarkHealthy calls
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.MarkHealthy(pool)
		}()
	}

	wg.Wait()

	// Should be healthy (counter should exceed threshold)
	state := tracker.GetState(pool)
	if state != "healthy" {
		t.Errorf("Pool should be healthy after concurrent MarkHealthy calls, got state: %s", state)
	}
}

func TestHealthTracker_ZeroSuccessesNeeded(t *testing.T) {
	// Test that zero or negative successesNeeded defaults to 1
	tracker := NewHealthTrackerWithSuccesses(100*time.Millisecond, 0)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	tracker.MarkBad(pool)
	time.Sleep(150 * time.Millisecond)

	// Should require at least 1 success
	tracker.MarkHealthy(pool)

	if tracker.GetState(pool) != "healthy" {
		t.Error("Pool should be healthy after 1 success (default minimum)")
	}
}

func TestHealthTracker_isTracking(t *testing.T) {
	// Test the isTracking helper method
	tracker := NewHealthTrackerWithSuccesses(100*time.Millisecond, 3)
	pool := &struct{ gorm.ConnPool }{} // Create distinct instance

	// Initially not tracking
	tracking, current, needed := tracker.isTracking(pool)
	if tracking {
		t.Error("Should not be tracking initially")
	}
	if current != 0 || needed != 3 {
		t.Errorf("Expected 0/3, got %d/%d", current, needed)
	}

	// Mark as bad
	tracker.MarkBad(pool)
	time.Sleep(150 * time.Millisecond)

	// Should be tracking
	tracking, current, needed = tracker.isTracking(pool)
	if !tracking {
		t.Error("Should be tracking after MarkBad")
	}
	if current != 0 || needed != 3 {
		t.Errorf("Expected 0/3, got %d/%d", current, needed)
	}

	// Add a success
	tracker.MarkHealthy(pool)

	tracking, current, needed = tracker.isTracking(pool)
	if !tracking {
		t.Error("Should still be tracking after 1 success")
	}
	if current != 1 || needed != 3 {
		t.Errorf("Expected 1/3, got %d/%d", current, needed)
	}

	// Fully recover
	tracker.MarkHealthy(pool)
	tracker.MarkHealthy(pool)

	tracking, current, needed = tracker.isTracking(pool)
	if tracking {
		t.Error("Should not be tracking after full recovery")
	}
	if current != 0 || needed != 3 {
		t.Errorf("Expected 0/3, got %d/%d", current, needed)
	}
}
