package config

// PipelineConfig defines the structure of the configuration file
type PipelineConfig struct {
	Pipeline struct {
		Source    SourceConfig       `yaml:"source"`
		Transform TransformConfig    `yaml:"transform"`
		Sink      SinkConfig         `yaml:"sink"`
		WAL       WALConfig          `yaml:"wal"`
		State     StateBackendConfig `yaml:"state_backend"`
		Partition PartitionConfig    `yaml:"partition"`
	} `yaml:"pipeline"`
}

// StateBackendConfig controls operator-state storage backend.
//
// Default behavior remains in-memory when Enabled=false.
type StateBackendConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"` // memory|kv|sqlite
	Path    string `yaml:"path"`

	// CacheMaxEntries is reserved for future hot-cache tuning.
	CacheMaxEntries int `yaml:"cache_max_entries"`

	// CheckpointMode is reserved for full|incremental state checkpoint strategy.
	CheckpointMode string `yaml:"checkpoint_mode"`

	// CheckpointEveryBatches is reserved for state backend checkpoint cadence.
	CheckpointEveryBatches int `yaml:"checkpoint_every_batches"`

	// MaxIncrementalMutationBytes forces full checkpoint when drained mutation payload
	// size reaches this threshold in incremental mode. 0 uses internal default.
	MaxIncrementalMutationBytes int `yaml:"max_incremental_mutation_bytes"`
}

// PartitionConfig controls partition fan-out execution mode.
//
// When Enabled is true, each Job is executed as an independent pipeline run,
// and sink/WAL paths are derived using Hive-style partition directories.
type PartitionConfig struct {
	Enabled bool `yaml:"enabled"`

	// Keys defines partition key order for hive directory layout.
	// Example: ["plant_id", "local_date"].
	Keys []string `yaml:"keys"`

	// Jobs is no longer supported.
	// When present in config, validation fails to enforce dynamic partitioning.
	Jobs []map[string]interface{} `yaml:"jobs"`
}

// WALConfig defines write-ahead log (WAL) settings.
// WAL stores input batches to enable crash recovery via replay.
type WALConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`

	// SQLitePragmas tune SQLite WAL memory usage and behavior.
	SQLitePragmas WALSQLitePragmaConfig `yaml:"sqlite_pragmas"`

	// CheckpointEveryBatches enables periodic operator-graph snapshots.
	// If 0, checkpointing is disabled.
	CheckpointEveryBatches int `yaml:"checkpoint_every_batches"`
}

// WALSQLitePragmaConfig exposes optional SQLite pragmas for WAL tuning.
// Empty values keep the SQLite defaults or internal WAL defaults.
type WALSQLitePragmaConfig struct {
	// TempStore controls temp storage: MEMORY | FILE | DEFAULT.
	TempStore string `yaml:"temp_store"`

	// CacheSize sets the page cache size. 0 keeps SQLite default.
	CacheSize int `yaml:"cache_size"`

	// MmapSize sets mmap size in bytes. 0 keeps SQLite default.
	MmapSize int64 `yaml:"mmap_size"`

	// BusyTimeoutMS sets the busy timeout in milliseconds. 0 keeps SQLite default.
	BusyTimeoutMS int `yaml:"busy_timeout_ms"`

	// ExtraPragmas allows arbitrary pragma overrides.
	// Example: {"journal_size_limit": "1048576"}
	ExtraPragmas map[string]string `yaml:"extra_pragmas"`
}

// SourceConfig defines the configuration for the data source
type SourceConfig struct {
	Type   string                 `yaml:"type"` // e.g., "csv"
	Config map[string]interface{} `yaml:"config"`
}

// TransformConfig defines the configuration for the transformation (SQL)
type TransformConfig struct {
	Type  string `yaml:"type"` // e.g., "sql"
	Query string `yaml:"query"`

	// TTL is a retention policy duration (e.g., "24h", "10s", "5 minutes").
	TTL string `yaml:"ttl"`

	// Watermark configures watermark/late-event handling for time windows.
	Watermark WatermarkYAMLConfig `yaml:"watermark"`
}

type WatermarkYAMLConfig struct {
	Enabled           bool   `yaml:"enabled"`
	MaxOutOfOrderness string `yaml:"max_out_of_orderness"` // e.g. "2s"
	AllowedLateness   string `yaml:"allowed_lateness"`     // e.g. "1s"
	Policy            string `yaml:"policy"`               // drop|buffer|emit
	MaxBufferSize     int    `yaml:"max_buffer_size"`
}

// SinkConfig defines the configuration for the data sink
type SinkConfig struct {
	Type   string                 `yaml:"type"` // e.g., "console"
	Config map[string]interface{} `yaml:"config"`
}

// CSVSourceConfig is a helper struct to parse the specific config for CSV source
type CSVSourceConfig struct {
	Path   string            `yaml:"path"`
	Schema map[string]string `yaml:"schema"` // column name -> type (int, float, string)
}

type HTTPSourceConfig struct {
	Port   int               `yaml:"port"`
	Path   string            `yaml:"path"`
	Schema map[string]string `yaml:"schema"`

	// AutoConvert enables automatic type conversion for fields defined in Schema.
	// When true, fields not in schema are passed through as raw JSON values.
	AutoConvert bool `yaml:"auto_convert"`

	// TimestampUnit defines specify timestamp resolution for numeric values.
	// Options: "auto" (default), "s", "ms", "us", "ns".
	TimestampUnit string `yaml:"timestamp_unit"`

	// BufferSize defines the number of pending batches buffered in memory.
	BufferSize      int `yaml:"buffer_size"`
	MaxBatchSize    int `yaml:"max_batch_size"`
	MaxBatchDelayMS int `yaml:"max_batch_delay_ms"`

	// MaxRequestBytes limits the maximum request body size for HTTP ingest.
	// 0 disables the limit.
	MaxRequestBytes int64 `yaml:"max_request_bytes"`

	// MaxBufferBytes limits the total buffered request body size in memory.
	// 0 disables the limit.
	MaxBufferBytes int64 `yaml:"max_buffer_bytes"`
}

type ChainSourceConfig struct {
	OnError string         `yaml:"on_error"`
	Sources []SourceConfig `yaml:"sources"`
}

// FileSinkConfig defines the configuration for the file sink
type FileSinkConfig struct {
	Path   string `yaml:"path"`
	Format string `yaml:"format"` // "json" or "csv"
}

// ParquetSinkConfig defines the configuration for the Parquet sink.
//
// Notes:
//   - Path is treated as an output prefix. Files are written as
//     <prefix>-<timestamp>-<seq>.parquet
//   - SchemaCachePath stores an inferred output schema (from SQL analysis) so it
//     can be reused on subsequent runs.
type ParquetSinkConfig struct {
	Path            string `yaml:"path"`
	SchemaCachePath string `yaml:"schema_cache_path"`

	// Compression: "zstd" | "snappy" | "gzip" | "uncompressed" (default: zstd)
	Compression string `yaml:"compression"`

	// RowGroupSize controls buffered rows per write call (default: 65536).
	RowGroupSize int `yaml:"row_group_size"`

	// RotateEveryBatches rotates files every N input batches (0 disables).
	RotateEveryBatches int `yaml:"rotate_every_batches"`
	// RotateEvery rotates files by time interval (e.g., "10s", "5 minutes"). Empty disables.
	RotateEvery string `yaml:"rotate_every"`
}

type HTTPPullSinkConfig struct {
	Port int    `yaml:"port"`
	Path string `yaml:"path"`

	// DiskSpillPath is the directory where partitioned snapshots are stored.
	DiskSpillPath string `yaml:"disk_spill_path"`
}
