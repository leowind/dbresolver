package dbresolver

import (
	"errors"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

const (
	Write Operation = "write"
	Read  Operation = "read"
)

type DBResolver struct {
	*gorm.DB
	configs          []Config
	resolvers        map[string]*resolver
	global           *resolver
	prepareStmtStore map[gorm.ConnPool]*gorm.PreparedStmtDB
	compileCallbacks []func(gorm.ConnPool) error
	once             int32
	// Health tracking for replicas
	healthTracker   *HealthTracker
	errorClassifier ErrorClassifier
	retryOnWriter   bool
	retryDelay      time.Duration
}

type Config struct {
	Sources           []gorm.Dialector
	Replicas          []gorm.Dialector
	Policy            Policy
	datas             []interface{}
	TraceResolverMode bool
	// FallbackToSourceOnNilPolicy enables automatic fallback to source/writer
	// when Policy.Resolve() returns nil (indicating no healthy replicas available).
	// Default: false (for backward compatibility)
	FallbackToSourceOnNilPolicy bool

	// HealthTracker tracks unhealthy replicas and excludes them from selection.
	// When a replica fails with a transient error, it's marked as bad for a cooldown period.
	// Works with CooldownPolicy to automatically avoid unhealthy replicas.
	// Optional: if nil, health tracking is disabled.
	HealthTracker *HealthTracker

	// ErrorClassifier determines which errors should mark a replica as unhealthy.
	// If nil, uses DefaultErrorClassifier which detects common transient errors.
	// Only used when HealthTracker is set.
	ErrorClassifier ErrorClassifier

	// RetryOnWriter enables automatic retry on writer when a replica fails.
	// When true, failed queries are automatically retried once on the writer.
	// When false, failed queries return errors immediately (next query will use healthy replica).
	// Default: false
	// Only used when HealthTracker is set.
	RetryOnWriter bool

	// RetryDelay is an optional delay before retrying on writer.
	// Useful during failover flaps to give the system time to stabilize.
	// Default: 0 (no delay)
	// Only used when RetryOnWriter is true.
	RetryDelay time.Duration
}

func Register(config Config, datas ...interface{}) *DBResolver {
	return (&DBResolver{}).Register(config, datas...)
}

func (dr *DBResolver) Register(config Config, datas ...interface{}) *DBResolver {
	if dr.prepareStmtStore == nil {
		dr.prepareStmtStore = map[gorm.ConnPool]*gorm.PreparedStmtDB{}
	}

	if dr.resolvers == nil {
		dr.resolvers = map[string]*resolver{}
	}

	if config.Policy == nil {
		config.Policy = RandomPolicy{}
	}

	// Set up health tracking if provided
	if config.HealthTracker != nil {
		dr.healthTracker = config.HealthTracker
		if config.ErrorClassifier == nil {
			dr.errorClassifier = DefaultErrorClassifier
		} else {
			dr.errorClassifier = config.ErrorClassifier
		}
		dr.retryOnWriter = config.RetryOnWriter
		dr.retryDelay = config.RetryDelay
	}

	config.datas = datas

	dr.configs = append(dr.configs, config)
	if dr.DB != nil {
		dr.compileConfig(config)
	}
	return dr
}

func (dr *DBResolver) Name() string {
	return "gorm:db_resolver"
}

func (dr *DBResolver) Initialize(db *gorm.DB) (err error) {
	if atomic.SwapInt32(&dr.once, 1) == 0 {
		dr.DB = db
		dr.registerCallbacks(db)
		if err = dr.registerHealthCallbacks(db); err != nil {
			return err
		}
		err = dr.compile()
	}
	return
}

func (dr *DBResolver) compile() error {
	for _, config := range dr.configs {
		if err := dr.compileConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func (dr *DBResolver) compileConfig(config Config) (err error) {
	var (
		connPool = dr.DB.Config.ConnPool
		r        = resolver{
			dbResolver:                  dr,
			policy:                      config.Policy,
			traceResolverMode:           config.TraceResolverMode,
			fallbackToSourceOnNilPolicy: config.FallbackToSourceOnNilPolicy,
		}
	)

	if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
		connPool = preparedStmtDB.ConnPool
	}

	if len(config.Sources) == 0 {
		r.sources = []gorm.ConnPool{connPool}
		dr.prepareStmtStore[connPool] = gorm.NewPreparedStmtDB(connPool, dr.PrepareStmtMaxSize, dr.PrepareStmtTTL)
	} else if r.sources, err = dr.convertToConnPool(config.Sources); err != nil {
		return err
	}

	if len(config.Replicas) == 0 {
		r.replicas = r.sources
	} else if r.replicas, err = dr.convertToConnPool(config.Replicas); err != nil {
		return err
	}

	if len(config.datas) > 0 {
		for _, data := range config.datas {
			if t, ok := data.(string); ok {
				dr.resolvers[t] = &r
			} else {
				stmt := &gorm.Statement{DB: dr.DB}
				if err := stmt.Parse(data); err == nil {
					dr.resolvers[stmt.Table] = &r
				} else {
					return err
				}
			}
		}
	} else if dr.global == nil {
		dr.global = &r
	} else {
		return errors.New("conflicted global resolver")
	}

	for _, fc := range dr.compileCallbacks {
		if err = r.call(fc); err != nil {
			return err
		}
	}

	if config.TraceResolverMode {
		dr.Logger = NewResolverModeLogger(dr.Logger)
	}

	return nil
}

func (dr *DBResolver) convertToConnPool(dialectors []gorm.Dialector) (connPools []gorm.ConnPool, err error) {
	config := *dr.DB.Config
	for _, dialector := range dialectors {
		if db, err := gorm.Open(dialector, &config); err == nil {
			connPool := db.ConnPool
			if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
				connPool = preparedStmtDB.ConnPool
			}

			dr.prepareStmtStore[connPool] = gorm.NewPreparedStmtDB(db.ConnPool, dr.PrepareStmtMaxSize, dr.PrepareStmtTTL)

			connPools = append(connPools, connPool)
		} else {
			return nil, err
		}
	}

	return connPools, err
}

func (dr *DBResolver) resolve(stmt *gorm.Statement, op Operation) gorm.ConnPool {
	if r := dr.getResolver(stmt); r != nil {
		return r.resolve(stmt, op)
	}
	return stmt.ConnPool
}

func (dr *DBResolver) getResolver(stmt *gorm.Statement) *resolver {
	if len(dr.resolvers) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := dr.resolvers[u.Use]; ok {
				return r
			}
		}

		if stmt.Table != "" {
			if r, ok := dr.resolvers[stmt.Table]; ok {
				return r
			}
		}

		if stmt.Model != nil {
			if err := stmt.Parse(stmt.Model); err == nil {
				if r, ok := dr.resolvers[stmt.Table]; ok {
					return r
				}
			}
		}

		if stmt.Schema != nil {
			if r, ok := dr.resolvers[stmt.Schema.Table]; ok {
				return r
			}
		}

		if rawSQL := stmt.SQL.String(); rawSQL != "" {
			if r, ok := dr.resolvers[getTableFromRawSQL(rawSQL)]; ok {
				return r
			}
		}
	}

	return dr.global
}
