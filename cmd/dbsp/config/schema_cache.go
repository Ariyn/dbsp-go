package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type ParquetSchema struct {
	Version   int             `json:"version"`
	QueryHash string          `json:"query_hash"`
	Columns   []ParquetColumn `json:"columns"`
}

type ParquetColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

const parquetSchemaVersion = 2

func InferOrLoadParquetSchema(query string, srcCfg SourceConfig, sinkCfg map[string]interface{}) (*ParquetSchema, error) {
	queryHash := hashString(query)
	if _, err := ParseHTTPPullSinkConfig(sinkCfg); err != nil {
		return nil, fmt.Errorf("sink configuration must be http_pull: %w", err)
	}

	cachePath := "http_pull.schema.json"

	if loaded, err := tryLoadSchemaCache(cachePath, queryHash); err == nil && loaded != nil {
		return loaded, nil
	}

	hints, err := extractSourceSchemaHints(srcCfg)
	if err != nil {
		return nil, err
	}

	schema := buildSchemaFromHints(hints)
	schema.Version = parquetSchemaVersion
	schema.QueryHash = queryHash

	if err := writeSchemaCache(cachePath, schema); err != nil {
		return nil, err
	}
	return schema, nil
}

func ParseHTTPPullSinkConfig(cfg map[string]interface{}) (*HTTPPullSinkConfig, error) {
	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal http_pull sink config: %w", err)
	}
	var out HTTPPullSinkConfig
	if err := yaml.Unmarshal(yamlBytes, &out); err != nil {
		return nil, fmt.Errorf("failed to parse http_pull sink config: %w", err)
	}
	if out.Port <= 0 {
		out.Port = 8080
	}
	if strings.TrimSpace(out.Path) == "" {
		out.Path = "/pull"
	}
	return &out, nil
}

func DecodeTo(in map[string]interface{}, out any) error {
	yamlBytes, err := yaml.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := yaml.Unmarshal(yamlBytes, out); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	return nil
}

func extractSourceSchemaHints(src SourceConfig) (map[string]string, error) {
	schema := map[string]string{}
	if strings.TrimSpace(src.Type) != "http" {
		return schema, nil
	}
	var cfg HTTPSourceConfig
	if err := DecodeTo(src.Config, &cfg); err != nil {
		return nil, err
	}
	for key, typ := range cfg.Schema {
		schema[key] = typ
	}
	return schema, nil
}

func buildSchemaFromHints(hints map[string]string) *ParquetSchema {
	keys := make([]string, 0, len(hints))
	for key := range hints {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	cols := make([]ParquetColumn, 0, len(keys)+1)
	for _, key := range keys {
		cols = append(cols, ParquetColumn{Name: key, Type: normalizeHintType(hints[key])})
	}
	cols = append(cols, ParquetColumn{Name: "__count", Type: "int64"})
	return &ParquetSchema{Columns: cols}
}

func normalizeHintType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "int", "int64", "timestamp":
		return "int64"
	case "float", "float64":
		return "float64"
	default:
		return "string"
	}
}

func tryLoadSchemaCache(path, queryHash string) (*ParquetSchema, error) {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s ParquetSchema
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if len(s.Columns) == 0 {
		return nil, fmt.Errorf("empty schema cache")
	}
	if s.Version == 0 {
		s.Version = 1
	}
	if s.Version == parquetSchemaVersion && (s.QueryHash == "" || s.QueryHash == queryHash) {
		return &s, nil
	}
	return nil, fmt.Errorf("schema cache mismatch")
}

func writeSchemaCache(path string, schema *ParquetSchema) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("mkdir schema cache dir: %w", err)
	}
	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		return fmt.Errorf("write schema cache %s: %w", path, err)
	}
	return nil
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
