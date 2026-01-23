package dbresolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ErrorClassifier determines if an error should cause a replica to be marked as bad.
type ErrorClassifier func(err error) bool

// DefaultErrorClassifier is the default implementation that detects common
// transient errors that indicate a replica is unhealthy.
func DefaultErrorClassifier(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	// pgx validation error (replica incorrectly configured as read-only)
	if strings.Contains(msg, "ValidateConnect failed") && strings.Contains(msg, "not read only") {
		return true
	}

	// Network-ish transient errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	l := strings.ToLower(msg)
	if strings.Contains(l, "connection refused") ||
		strings.Contains(l, "i/o timeout") ||
		strings.Contains(l, "no such host") ||
		strings.Contains(l, "broken pipe") ||
		strings.Contains(l, "connection reset by peer") ||
		strings.Contains(l, "server closed the connection") ||
		strings.Contains(l, "eof") {
		return true
	}

	return false
}

// registerHealthCallbacks registers GORM callbacks that track replica health.
// When a query fails on a replica with a transient error, the replica is marked as bad.
// When a query succeeds on a previously-bad replica, it's unmarked (recovered).
func (dr *DBResolver) registerHealthCallbacks(db *gorm.DB) error {
	if dr.healthTracker == nil || dr.errorClassifier == nil {
		return nil // Health tracking not enabled
	}

	// Track errors after Query callback (Find, First, Take, etc.)
	if err := db.Callback().Query().After("gorm:query").
		Register("dbresolver:track_query_health", dr.trackQueryHealth); err != nil {
		return err
	}

	// Track errors after Row callback (db.Raw().Scan())
	if err := db.Callback().Row().After("gorm:row").
		Register("dbresolver:track_row_health", dr.trackRowHealth); err != nil {
		return err
	}

	// Track successes to unmark recovered replicas
	if err := db.Callback().Query().After("dbresolver:track_query_health").
		Register("dbresolver:track_query_success", dr.trackQuerySuccess); err != nil {
		return err
	}

	if err := db.Callback().Row().After("dbresolver:track_row_health").
		Register("dbresolver:track_row_success", dr.trackRowSuccess); err != nil {
		return err
	}

	return nil
}

// trackQueryHealth marks a replica as bad if a query fails with a transient error
func (dr *DBResolver) trackQueryHealth(tx *gorm.DB) {
	if tx == nil || tx.Statement == nil || tx.Error == nil {
		return
	}

	// Only track read operations (not writes)
	if !looksLikeSelectOp(tx.Statement) {
		return
	}

	// Don't track errors within transactions (can't switch mid-transaction)
	if isSQLTx(tx.Statement.ConnPool) {
		return
	}

	// Check if this error indicates an unhealthy replica
	if !dr.errorClassifier(tx.Error) {
		return
	}

	// Mark the bad replica
	if tx.Statement.ConnPool != nil {
		dr.healthTracker.MarkBad(tx.Statement.ConnPool)
		tx.Logger.Warn(tx.Statement.Context, "Marked replica as unhealthy temporary: %v", tx.Error)
	}

	// Retry on writer if enabled
	if !dr.retryOnWriter || alreadyRetried(tx.Statement.Context) {
		return
	}

	// Capture Dest immediately while it's still available
	dest := tx.Statement.Dest
	if isNilInterface(dest) {
		return
	}

	tx.Logger.Info(tx.Statement.Context, "Retrying query on writer after replica failure")

	// Clear error before retry
	tx.Error = nil

	// Optional delay before retry
	if dr.retryDelay > 0 {
		time.Sleep(dr.retryDelay)
	}

	// Mark context as retried to prevent infinite loops
	tx.Statement.Context = markRetried(tx.Statement.Context)

	// Force writer routing
	tx.Clauses(Write)

	// Ensure we don't reuse the failing pool
	tx.Statement.ConnPool = nil

	// Keep the existing Dest for the retry
	tx.Statement.Dest = dest

	// Re-execute the Query callback
	tx.Callback().Query().Execute(tx)
}

// trackRowHealth marks a replica as bad if a Raw().Scan() fails with a transient error
func (dr *DBResolver) trackRowHealth(tx *gorm.DB) {
	if tx == nil || tx.Statement == nil || tx.Error == nil {
		return
	}

	// Only track read operations
	if !looksLikeSelectOp(tx.Statement) {
		return
	}

	// Don't track errors within transactions
	if isSQLTx(tx.Statement.ConnPool) {
		return
	}

	// Check if this error indicates an unhealthy replica
	if !dr.errorClassifier(tx.Error) {
		return
	}

	// Mark the bad replica
	if tx.Statement.ConnPool != nil {
		dr.healthTracker.MarkBad(tx.Statement.ConnPool)
		tx.Logger.Warn(tx.Statement.Context, "Marked replica as unhealthy temporary: %v", tx.Error)
	}

	// Retry on writer if enabled
	if !dr.retryOnWriter || alreadyRetried(tx.Statement.Context) {
		return
	}

	tx.Logger.Info(tx.Statement.Context, "Retrying row query on writer after replica failure")

	// Clear error before retry
	tx.Error = nil

	// Optional delay before retry
	if dr.retryDelay > 0 {
		time.Sleep(dr.retryDelay)
	}

	// Mark context as retried to prevent infinite loops
	tx.Statement.Context = markRetried(tx.Statement.Context)

	// Force writer routing
	tx.Clauses(Write)

	// Ensure we don't reuse the failing pool
	tx.Statement.ConnPool = nil
	tx.Statement.Dest = nil

	tx.Set("rows", true)
	tx.Callback().Row().Execute(tx)

	// Check for errors in the retry
	switch d := tx.Statement.Dest.(type) {
	case *sql.Rows:
		if d != nil && d.Err() != nil {
			tx.Error = d.Err()
		}
	case nil:
		tx.Error = errors.New("no rows returned from retry")
	default:
		tx.Error = fmt.Errorf("unexpected dest type %T", tx.Statement.Dest)
	}
}

// looksLikeSelectOp returns true if the statement appears to be a read operation
func looksLikeSelectOp(stmt *gorm.Statement) bool {
	if stmt == nil {
		return false
	}

	sqlText := strings.TrimSpace(strings.ToUpper(stmt.SQL.String()))
	if sqlText == "" {
		// Query/Row callbacks are typically read paths
		return true
	}

	return strings.HasPrefix(sqlText, "SELECT") ||
		strings.HasPrefix(sqlText, "WITH") ||
		strings.HasPrefix(sqlText, "SHOW") ||
		strings.HasPrefix(sqlText, "EXPLAIN")
}

// isSQLTx returns true if the pool is a SQL transaction
func isSQLTx(pool gorm.ConnPool) bool {
	if pool == nil {
		return false
	}
	_, ok := pool.(*sql.Tx)
	return ok
}

// Context key for tracking retries
type ctxKey string

const retriedKey ctxKey = "dbresolver_retried"

// alreadyRetried checks if a context has already been retried
func alreadyRetried(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(retriedKey)
	b, _ := v.(bool)
	return b
}

// markRetried marks a context as having been retried
func markRetried(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, retriedKey, true)
}

// isNilInterface checks if an interface value is nil or contains a nil pointer
func isNilInterface(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// trackQuerySuccess unmarks a replica as bad when a query succeeds on it
func (dr *DBResolver) trackQuerySuccess(tx *gorm.DB) {
	if tx == nil || tx.Statement == nil {
		return
	}

	// Only track successful read operations
	if tx.Error != nil {
		return
	}

	if !looksLikeSelectOp(tx.Statement) {
		return
	}

	// Don't track transactions
	if isSQLTx(tx.Statement.ConnPool) {
		return
	}

	// If this pool was marked as bad and the query succeeded, unmark it
	if tx.Statement.ConnPool != nil && dr.healthTracker.IsBad(tx.Statement.ConnPool) {
		dr.healthTracker.MarkHealthy(tx.Statement.ConnPool)
		tx.Logger.Info(tx.Statement.Context, "Replica recovered and unmarked as healthy")
	}
}

// trackRowSuccess unmarks a replica as bad when a row query succeeds on it
func (dr *DBResolver) trackRowSuccess(tx *gorm.DB) {
	if tx == nil || tx.Statement == nil {
		return
	}

	// Only track successful read operations
	if tx.Error != nil {
		return
	}

	if !looksLikeSelectOp(tx.Statement) {
		return
	}

	// Don't track transactions
	if isSQLTx(tx.Statement.ConnPool) {
		return
	}

	// If this pool was marked as bad and the query succeeded, unmark it
	if tx.Statement.ConnPool != nil && dr.healthTracker.IsBad(tx.Statement.ConnPool) {
		dr.healthTracker.MarkHealthy(tx.Statement.ConnPool)
		tx.Logger.Info(tx.Statement.Context, "Replica recovered and unmarked as healthy")
	}
}
