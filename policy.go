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
type HealthTracker struct {
	cooldown time.Duration
	mu       sync.RWMutex
	bad      map[string]time.Time // pool key -> expiration time
}

// NewHealthTracker creates a new HealthTracker with the specified cooldown duration.
// Cooldown is the duration that a pool remains marked as unhealthy.
func NewHealthTracker(cooldown time.Duration) *HealthTracker {
	return &HealthTracker{
		cooldown: cooldown,
		bad:      make(map[string]time.Time),
	}
}

// key generates a stable identity string for a connection pool
func (t *HealthTracker) key(pool gorm.ConnPool) string {
	return fmt.Sprintf("%T:%p", pool, pool)
}

// MarkBad marks a connection pool as unhealthy for the cooldown duration.
func (t *HealthTracker) MarkBad(pool gorm.ConnPool) {
	if pool == nil {
		return
	}
	key := t.key(pool)
	until := time.Now().Add(t.cooldown)

	t.mu.Lock()
	t.bad[key] = until
	t.mu.Unlock()
}

// MarkHealthy removes a connection pool from the unhealthy list.
// This is called when a previously-bad replica successfully handles a query,
// indicating it has recovered before the cooldown period expired.
func (t *HealthTracker) MarkHealthy(pool gorm.ConnPool) {
	if pool == nil {
		return
	}
	key := t.key(pool)

	t.mu.Lock()
	delete(t.bad, key)
	t.mu.Unlock()
}

// IsBad checks if a connection pool is currently marked as unhealthy.
func (t *HealthTracker) IsBad(pool gorm.ConnPool) bool {
	if pool == nil {
		return false
	}
	key := t.key(pool)

	t.mu.RLock()
	until, ok := t.bad[key]
	t.mu.RUnlock()

	if !ok {
		return false
	}
	if time.Now().After(until) {
		// lazy cleanup of expired entries
		t.mu.Lock()
		delete(t.bad, key)
		t.mu.Unlock()
		return false
	}
	return true
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
