package dbresolver

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// Policy defines the interface for connection pool selection strategies.
// Implementations can return nil to signal that no healthy connection pools
// are available, which will cause dbresolver to fall back to the writer/source.
type Policy interface {
	Resolve([]gorm.ConnPool) gorm.ConnPool
}

type PolicyFunc func([]gorm.ConnPool) gorm.ConnPool

func (f PolicyFunc) Resolve(connPools []gorm.ConnPool) gorm.ConnPool {
	return f(connPools)
}

type RandomPolicy struct {
}

func (RandomPolicy) Resolve(connPools []gorm.ConnPool) gorm.ConnPool {
	if len(connPools) == 0 {
		return nil
	}
	return connPools[rand.Intn(len(connPools))]
}

func RoundRobinPolicy() Policy {
	var i int
	return PolicyFunc(func(connPools []gorm.ConnPool) gorm.ConnPool {
		if len(connPools) == 0 {
			return nil
		}
		i = (i + 1) % len(connPools)
		return connPools[i]
	})
}

func StrictRoundRobinPolicy() Policy {
	var i int64
	return PolicyFunc(func(connPools []gorm.ConnPool) gorm.ConnPool {
		if len(connPools) == 0 {
			return nil
		}
		return connPools[int(atomic.AddInt64(&i, 1))%len(connPools)]
	})
}

// HealthTracker tracks unhealthy connection pools with a cooldown period.
// Pools marked as bad are excluded from selection for the cooldown duration.
// After the cooldown period expires, pools enter a "half-open" state where they
// can be tested with queries. They are fully recovered after achieving the required
// number of consecutive successful queries.
type HealthTracker struct {
	cooldown           time.Duration
	successesNeeded    int // Number of consecutive successes required for recovery
	mu                 sync.RWMutex
	bad                map[string]time.Time // pool key -> expiration time
	consecutiveSuccess map[string]int       // pool key -> consecutive success count
}

// NewHealthTracker creates a new HealthTracker with the specified cooldown duration.
// Uses default setting of 1 consecutive success required (immediate recovery).
// For more control over recovery behavior, use NewHealthTrackerWithSuccesses.
//
// Cooldown is the duration that a pool remains marked as unhealthy before entering
// the half-open state where it can be probed.
func NewHealthTracker(cooldown time.Duration) *HealthTracker {
	return NewHealthTrackerWithSuccesses(cooldown, 1)
}

// NewHealthTrackerWithSuccesses creates a new HealthTracker with the specified cooldown duration
// and number of consecutive successes needed before marking a replica as healthy.
//
// Parameters:
//   - cooldown: Duration that a pool remains in cooldown after being marked as bad.
//     After this period, the pool enters "half-open" state and can be tested.
//   - successesNeeded: Number of consecutive successful queries required to mark as healthy.
//     Set to 1 for immediate recovery (current behavior, backward compatible).
//     Set to 3-5 for production resilience against flapping.
//     Set to 5-10 for maximum stability in unstable network conditions.
//
// Example:
//
//	tracker := dbresolver.NewHealthTrackerWithSuccesses(30*time.Second, 3)
func NewHealthTrackerWithSuccesses(cooldown time.Duration, successesNeeded int) *HealthTracker {
	if successesNeeded < 1 {
		successesNeeded = 1 // Ensure at least 1 success is required
	}
	return &HealthTracker{
		cooldown:           cooldown,
		successesNeeded:    successesNeeded,
		bad:                make(map[string]time.Time),
		consecutiveSuccess: make(map[string]int),
	}
}

// key generates a stable identity string for a connection pool
func (t *HealthTracker) key(pool gorm.ConnPool) string {
	return fmt.Sprintf("%T:%p", pool, pool)
}

// MarkBad marks a connection pool as unhealthy for the cooldown duration.
// This also resets any consecutive success counter for the pool.
func (t *HealthTracker) MarkBad(pool gorm.ConnPool) {
	if pool == nil {
		return
	}
	key := t.key(pool)
	until := time.Now().Add(t.cooldown)

	t.mu.Lock()
	t.bad[key] = until
	delete(t.consecutiveSuccess, key) // Reset success counter on failure
	t.mu.Unlock()
}

// MarkHealthy is called when a query succeeds on a pool.
// If the pool is in the bad list (including half-open state after cooldown):
//   - Increments consecutive success counter
//   - If counter reaches successesNeeded, removes from bad list (fully recovered)
//
// If the pool is not in the bad list, this is a no-op.
func (t *HealthTracker) MarkHealthy(pool gorm.ConnPool) {
	if pool == nil {
		return
	}
	key := t.key(pool)

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if pool is in bad list (either still in cooldown or half-open)
	_, isBad := t.bad[key]
	if !isBad {
		return // Pool is already healthy, nothing to do
	}

	// Increment consecutive success counter
	t.consecutiveSuccess[key]++

	// Check if we've reached the threshold
	if t.consecutiveSuccess[key] >= t.successesNeeded {
		// Fully recovered - remove from bad list and clean up counter
		delete(t.bad, key)
		delete(t.consecutiveSuccess, key)
	}
}

// IsBad checks if a connection pool is currently marked as unhealthy.
// Returns true only during the cooldown period.
// After cooldown expires, the pool enters "half-open" state where it can be probed
// but still tracked in the bad map until it achieves consecutive successes.
func (t *HealthTracker) IsBad(pool gorm.ConnPool) bool {
	if pool == nil {
		return false
	}
	key := t.key(pool)

	t.mu.RLock()
	until, ok := t.bad[key]
	t.mu.RUnlock()

	if !ok {
		return false // Not in bad list, fully healthy
	}

	// Check if cooldown has expired (half-open state)
	if time.Now().After(until) {
		// Cooldown expired - allow probing (return false)
		// Pool remains in bad map but can be selected for testing
		// Will be fully removed after successesNeeded consecutive successes
		return false
	}

	// Still in cooldown period - exclude from selection
	return true
}

// GetState returns the current state of a pool for debugging and monitoring purposes.
// Returns one of: "healthy", "bad", or "probing (N/M)" where N is current consecutive
// successes and M is the total needed for recovery.
//
// This method is useful for:
//   - Debugging health tracking behavior
//   - Understanding why a replica is or isn't being used
func (t *HealthTracker) GetState(pool gorm.ConnPool) string {
	if pool == nil {
		return "healthy"
	}
	key := t.key(pool)

	t.mu.RLock()
	until, isBad := t.bad[key]
	successCount := t.consecutiveSuccess[key]
	t.mu.RUnlock()

	if !isBad {
		return "healthy"
	}

	if time.Now().After(until) {
		return fmt.Sprintf("probing (%d/%d)", successCount, t.successesNeeded)
	}

	return "bad"
}

// isTracking checks if a pool is being tracked (in bad list, including half-open state).
// Returns true if the pool is in the bad list, false otherwise.
// Also returns the current success count and total successes needed.
func (t *HealthTracker) isTracking(pool gorm.ConnPool) (tracking bool, currentSuccesses int, neededSuccesses int) {
	if pool == nil {
		return false, 0, 0
	}
	key := t.key(pool)

	t.mu.RLock()
	_, tracking = t.bad[key]
	currentSuccesses = t.consecutiveSuccess[key]
	neededSuccesses = t.successesNeeded
	t.mu.RUnlock()

	return
}

// CooldownPolicy is a connection pool selection policy that filters out unhealthy pools.
// It uses a HealthTracker to maintain a list of bad pools with a cooldown period.
//
// When all replicas are marked as bad:
// - If FallbackToWriter is true: returns nil to signal dbresolver to use writer (requires FallbackToSourceOnNilPolicy=true)
// - If FallbackToWriter is false: falls back to RandomPolicy and selects from all pools (old behavior)
type CooldownPolicy struct {
	tracker          *HealthTracker
	FallbackToWriter bool
	randomPolicy     RandomPolicy
}

// NewCooldownPolicy creates a new CooldownPolicy with the specified health tracker.
//
// Parameters:
//   - tracker: HealthTracker to use for marking and checking pool health
//   - fallbackToWriter: When true, returns nil when all replicas are bad (signals to use writer).
//     When false, falls back to RandomPolicy (old behavior).
//
// Note: When fallbackToWriter is true, you must also set FallbackToSourceOnNilPolicy=true
// in the dbresolver Config for the fallback to work.
func NewCooldownPolicy(tracker *HealthTracker, fallbackToWriter bool) *CooldownPolicy {
	return &CooldownPolicy{
		tracker:          tracker,
		FallbackToWriter: fallbackToWriter,
		randomPolicy:     RandomPolicy{},
	}
}

// Resolve selects a healthy connection pool using random selection.
// Pools marked as bad in the tracker are excluded from selection.
func (p *CooldownPolicy) Resolve(pools []gorm.ConnPool) gorm.ConnPool {
	healthy := make([]gorm.ConnPool, 0, len(pools))
	for _, pool := range pools {
		if !p.tracker.IsBad(pool) {
			healthy = append(healthy, pool)
		}
	}

	if len(healthy) > 0 {
		// Select randomly from healthy pools
		return healthy[rand.Intn(len(healthy))]
	}

	// All pools are marked as bad
	if p.FallbackToWriter {
		// Return nil to signal dbresolver: no healthy replicas, fall back to writer
		// Requires FallbackToSourceOnNilPolicy=true in Config
		return nil
	}

	// Old behavior: try any pool (even if marked as bad)
	return p.randomPolicy.Resolve(pools)
}
