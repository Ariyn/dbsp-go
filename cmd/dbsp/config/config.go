package config

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func ParsePipelineConfig(configFile []byte) (PipelineConfig, error) {
	var cfg PipelineConfig
	dec := yaml.NewDecoder(bytes.NewReader(configFile))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return PipelineConfig{}, err
	}
	return cfg, nil
}

func DecodeTo(in map[string]interface{}, out any) error {
	return decodeTo(in, out)
}

func ApplyMemoryLimit(raw string) error {
	limit := strings.TrimSpace(raw)
	if limit == "" {
		limit = "1GiB"
	}
	bytes, err := parseHumanBytes(limit)
	if err != nil {
		return err
	}
	if bytes <= 0 {
		bytes, err = parseHumanBytes("1GiB")
		if err != nil {
			return err
		}
		limit = "1GiB"
	}
	debug.SetMemoryLimit(bytes)
	fmt.Printf("Applied Go memory limit: %s (%d bytes)\n", limit, bytes)
	return nil
}

func ParseHumanBytes(raw string) (int64, error) {
	return parseHumanBytes(raw)
}

func parseHumanBytes(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("negative byte size is not supported: %q", raw)
	}

	idx := 0
	for idx < len(s) {
		c := s[idx]
		if (c >= '0' && c <= '9') || c == '.' {
			idx++
			continue
		}
		break
	}
	if idx == 0 {
		return 0, fmt.Errorf("invalid byte size: %q", raw)
	}

	numPart := strings.TrimSpace(s[:idx])
	unitPart := strings.ToUpper(strings.TrimSpace(s[idx:]))
	if unitPart == "" {
		unitPart = "B"
	}

	value, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("negative byte size is not supported: %q", raw)
	}

	mult, ok := map[string]float64{
		"B":   1,
		"K":   1 << 10,
		"KB":  1 << 10,
		"KIB": 1 << 10,
		"M":   1 << 20,
		"MB":  1 << 20,
		"MIB": 1 << 20,
		"G":   1 << 30,
		"GB":  1 << 30,
		"GIB": 1 << 30,
		"T":   1 << 40,
		"TB":  1 << 40,
		"TIB": 1 << 40,
		"P":   1 << 50,
		"PB":  1 << 50,
		"PIB": 1 << 50,
	}[unitPart]
	if !ok {
		return 0, fmt.Errorf("unknown byte size unit: %q", unitPart)
	}

	return int64(value * mult), nil
}

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

	// GOMAXPROCS overrides Go runtime parallelism. 0 uses all available CPUs.
	GOMAXPROCS int `yaml:"gomaxprocs"`

	// MemoryLimit sets a soft Go heap limit (e.g., "1GiB").
	MemoryLimit string `yaml:"memory_limit"`

	// StateTTL expires operator state entries after this duration (e.g., "1h").
	StateTTL string `yaml:"state_ttl"`

	// OnlyLastLag keeps only the latest retained row for offset-1 LAG state.
	// Default false preserves the full ordered state required for general updates.
	OnlyLastLag bool `yaml:"only_last_lag"`

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

	// SQLCompliance tunes parser compliance and fallback behaviors.
	SQLCompliance SQLComplianceConfig `yaml:"sql_compliance"`

	// TTL is a retention policy duration (e.g., "24h", "10s", "5 minutes").
	TTL string `yaml:"ttl"`

	// Watermark configures watermark/late-event handling for time windows.
	Watermark WatermarkYAMLConfig `yaml:"watermark"`
}

type SQLComplianceConfig struct {
	StrictValidation bool `yaml:"strict_validation"`
	FallbackWarn     bool `yaml:"fallback_warn"`
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

	// WAL configuration
	WALDir          string `yaml:"wal_dir"`
	WALSegmentSize  string `yaml:"wal_segment_size"`
	WALMaxTotalSize string `yaml:"wal_max_total_size"`

	// Checkpoint configuration
	CheckpointDir      string `yaml:"checkpoint_dir"`
	CheckpointInterval string `yaml:"checkpoint_interval"`
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
