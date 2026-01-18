package main

// PipelineConfig defines the structure of the configuration file
type PipelineConfig struct {
	Pipeline struct {
		Source    SourceConfig    `yaml:"source"`
		Transform TransformConfig `yaml:"transform"`
		Sink      SinkConfig      `yaml:"sink"`
		WAL       WALConfig       `yaml:"wal"`
	} `yaml:"pipeline"`
}

// WALConfig defines write-ahead log (WAL) settings.
// WAL stores input batches to enable crash recovery via replay.
type WALConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`

	// CheckpointEveryBatches enables periodic operator-graph snapshots.
	// If 0, checkpointing is disabled.
	CheckpointEveryBatches int `yaml:"checkpoint_every_batches"`
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

	// JoinTTL is an optional processing-time TTL for join state (e.g., "10s", "5 minutes").
	JoinTTL string `yaml:"join_ttl"`

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

// FileSinkConfig defines the configuration for the file sink
type FileSinkConfig struct {
	Path   string `yaml:"path"`
	Format string `yaml:"format"` // "json" or "csv"
}
