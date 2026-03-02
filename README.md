# DBResolver

DBResolver adds multiple databases support to GORM, the following features are supported:

* Multiple sources, replicas
* Read/Write Splitting
* Automatic connection switching based on the working table/struct
* Manual connection switching
* Sources/Replicas load balancing
* Works for RAW SQL
* Transaction

## Quick Start

```go
import (
  "gorm.io/gorm"
  "gorm.io/plugin/dbresolver"
  "gorm.io/driver/mysql"
)

DB, err := gorm.Open(mysql.Open("db1_dsn"), &gorm.Config{})

DB.Use(dbresolver.Register(dbresolver.Config{
  // use `db2` as sources, `db3`, `db4` as replicas
  Sources:  []gorm.Dialector{mysql.Open("db2_dsn")},
  Replicas: []gorm.Dialector{mysql.Open("db3_dsn"), mysql.Open("db4_dsn")},
  // sources/replicas load balancing policy
  Policy: dbresolver.RandomPolicy{},
  // print sources/replicas mode in logger
  ResolverModeReplica: true,
}).Register(dbresolver.Config{
  // use `db1` as sources (DB's default connection), `db5` as replicas for `User`, `Address`
  Replicas: []gorm.Dialector{mysql.Open("db5_dsn")},
}, &User{}, &Address{}).Register(dbresolver.Config{
  // use `db6`, `db7` as sources, `db8` as replicas for `orders`, `Product`
  Sources:  []gorm.Dialector{mysql.Open("db6_dsn"), mysql.Open("db7_dsn")},
  Replicas: []gorm.Dialector{mysql.Open("db8_dsn")},
}, "orders", &Product{}, "secondary"))
```

### Automatic connection switching

DBResolver will automatically switch connections based on the working table/struct

For RAW SQL, DBResolver will extract the table name from the SQL to match the resolver, and will use `sources` unless the SQL begins with `SELECT`, for example:

```go
// `User` Resolver Examples
DB.Table("users").Rows() // replicas `db5`
DB.Model(&User{}).Find(&AdvancedUser{}) // replicas `db5`
DB.Exec("update users set name = ?", "jinzhu") // sources `db1`
DB.Raw("select name from users").Row().Scan(&name) // replicas `db5`
DB.Create(&user) // sources `db1`
DB.Delete(&User{}, "name = ?", "jinzhu") // sources `db1`
DB.Table("users").Update("name", "jinzhu") // sources `db1`

// Global Resolver Examples
DB.Find(&Pet{}) // replicas `db3`/`db4`
DB.Save(&Pet{}) // sources `db2`

// Orders Resolver Examples
DB.Find(&Order{}) // replicas `db8`
DB.Table("orders").Find(&Report{}) // replicas `db8`
```

### Read/Write Splitting

Read/Write splitting with DBResolver based on the current using [GORM callback](https://gorm.io/docs/write_plugins.html).

For `Query`, `Row` callback, will use `replicas` unless `Write` mode specified
For `Raw` callback, statements are considered read-only and will use `replicas` if the SQL starts with `SELECT`

### Manual connection switching

```go
// Use Write Mode: read user from sources `db1`
DB.Clauses(dbresolver.Write).First(&user)

// Specify Resolver: read user from `secondary`'s replicas: db8
DB.Clauses(dbresolver.Use("secondary")).First(&user)

// Specify Resolver and Write Mode: read user from `secondary`'s sources: db6 or db7
DB.Clauses(dbresolver.Use("secondary"), dbresolver.Write).First(&user)
```

### Transaction

When using transaction, DBResolver will keep using the transaction and won't switch to sources/replicas based on configuration

But you can specifies which DB to use before starting a transaction, for example:

```go
// Start transaction based on default replicas db
tx := DB.Clauses(dbresolver.Read).Begin()

// Start transaction based on default sources db
tx := DB.Clauses(dbresolver.Write).Begin()

// Start transaction based on `secondary`'s sources
tx := DB.Clauses(dbresolver.Use("secondary"), dbresolver.Write).Begin()
```

### Load Balancing

GORM supports load balancing sources/replicas based on policy, the policy is an interface implements following interface:

```go
type Policy interface {
	Resolve([]gorm.ConnPool) gorm.ConnPool
}
```

Currently only the `RandomPolicy` implemented and it is the default option if no policy specified.

### Health Tracking with Consecutive Success Requirements

DBResolver can track replica health and automatically avoid unhealthy replicas. To prevent flapping during intermittent failures, you can configure the health tracker to require multiple consecutive successes before marking a replica as healthy again.

```go
import (
  "time"
  "gorm.io/gorm"
  "gorm.io/plugin/dbresolver"
  "gorm.io/driver/mysql"
)

// Create health tracker requiring 3 consecutive successes
tracker := dbresolver.NewHealthTrackerWithSuccesses(
  30 * time.Second,  // Cooldown period before probing
  3,                 // Consecutive successes needed
)

// Create policy with fallback to writer when all replicas are unhealthy
policy := dbresolver.NewCooldownPolicy(tracker, true)

DB.Use(dbresolver.Register(dbresolver.Config{
  Replicas:                    []gorm.Dialector{mysql.Open("replica1"), mysql.Open("replica2")},
  Policy:                      policy,
  HealthTracker:               tracker,
  ErrorClassifier:             dbresolver.DefaultErrorClassifier,
  FallbackToSourceOnNilPolicy: true,
  RetryOnWriter:               true,
}))
```

**How it works:**

1. **Failure → Bad**: When a replica fails with a transient error, it's marked as bad and excluded from the pool for the cooldown period
2. **Cooldown → Probing**: After cooldown expires, the replica enters "half-open" state and can be selected for testing
3. **Testing**: Each successful query increments the consecutive success counter; any failure resets the counter and restarts cooldown
4. **Recovery**: After N consecutive successes, the replica is fully restored to healthy state

**Recommended settings:**

- **Fast failover**: `successesNeeded: 2` - Quick recovery with minimal verification
- **Production (recommended)**: `successesNeeded: 3-5` - Balance between stability and recovery time
- **Unstable networks**: `successesNeeded: 5-10` - Maximum resilience against flapping

**State transitions:**

```
Healthy ─[failure]→ Bad (cooldown) ─[time expires]→ Half-Open (probing)
   ↑                                                       │
   │                                                       │
   └─────[N consecutive successes]─────────────────────────┘
                                                          │
                                                     [failure]
                                                          │
                                                          ↓
                                                    Bad (cooldown)
```

**Backward compatible:**

For immediate recovery on first success (original behavior), use the standard constructor:

```go
tracker := dbresolver.NewHealthTracker(30 * time.Second)
```

### Connection Pool

```go
DB.Use(
  dbresolver.Register(dbresolver.Config{ /* xxx */ }).
  SetConnMaxIdleTime(time.Hour).
  SetConnMaxLifetime(24 * time.Hour).
  SetMaxIdleConns(100).
  SetMaxOpenConns(200)
)
```
