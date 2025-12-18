package main

// PipelineConfig defines the structure of the configuration file
type PipelineConfig struct {
	Pipeline struct {
		Source    SourceConfig    `yaml:"source"`
		Transform TransformConfig `yaml:"transform"`
		Sink      SinkConfig      `yaml:"sink"`
	} `yaml:"pipeline"`
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
