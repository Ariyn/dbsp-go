package config

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// PipelineConfig defines the structure of the configuration file
type PipelineConfig struct {
	Pipeline struct {
		Source    SourceConfig       `yaml:"source"`
		Transform TransformConfig    `yaml:"transform"`
		Sink      SinkConfig         `yaml:"sink"`
		State     StateBackendConfig `yaml:"state_backend"`
	} `yaml:"pipeline"`
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

type StateBackendConfig struct {
	MemoryLimit string `yaml:"memory_limit"`
}

type SourceConfig struct {
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

type TransformConfig struct {
	Type  string `yaml:"type"`
	Query string `yaml:"query"`
}

type SinkConfig struct {
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

// HTTPSourceConfig defines the configuration for the HTTP source.
type HTTPSourceConfig struct {
	Port   int               `yaml:"port"`
	Path   string            `yaml:"path"`
	Schema map[string]string `yaml:"schema"`

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
}

type HTTPPullSinkConfig struct {
	Port int    `yaml:"port"`
	Path string `yaml:"path"`
}
