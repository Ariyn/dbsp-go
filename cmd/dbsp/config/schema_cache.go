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

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	sqlconv "github.com/ariyn/dbsp/internal/dbsp/sql"
	"gopkg.in/yaml.v3"
)

// ParquetSchema is a small, stable schema description used to build an Arrow schema
// and to cache the inferred schema on disk.
//
// Type is a compact string enum: "string" | "int64" | "float64".
// Unknown/unsupported runtime values are stringified.
//
// NOTE: This schema describes sink *rows*, not the input table schema.
// It always includes "__count" which stores TupleDelta.Count.
type ParquetSchema struct {
	Version   int             `json:"version"`
	QueryHash string          `json:"query_hash"`
	Columns   []ParquetColumn `json:"columns"`
}

type ParquetColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func InferOrLoadParquetSchema(query string, srcCfg SourceConfig, sinkCfg map[string]interface{}) (*ParquetSchema, error) {
	queryHash := hashString(query)
	var cachePath string

	if pcfg, err := ParseParquetSinkConfig(sinkCfg); err == nil {
		cachePath = strings.TrimSpace(pcfg.SchemaCachePath)
		if cachePath == "" {
			cachePath = defaultParquetSchemaCachePath(pcfg.Path)
		}
	} else if hcfg, err := ParseHTTPPullSinkConfig(sinkCfg); err == nil {
		// For http_pull, optionally use a cache if configured, or default.
		cachePath = "http_pull.schema.json"
		if hcfg.DiskSpillPath != "" {
			cachePath = filepath.Join(hcfg.DiskSpillPath, "schema.json")
		}
	} else {
		return nil, fmt.Errorf("sink configuration must be either parquet or http_pull to use ParquetSchema")
	}

	if st, err := os.Stat(cachePath); err == nil && !st.IsDir() {
		b, err := os.ReadFile(cachePath)
		if err != nil {
			return nil, fmt.Errorf("read schema cache %s: %w", cachePath, err)
		}
		var s ParquetSchema
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, fmt.Errorf("parse schema cache %s: %w", cachePath, err)
		}
		if s.Version == 0 {
			s.Version = 1
		}
		if s.QueryHash != "" && s.QueryHash != queryHash {
			// Query changed? We should re-infer.
		} else if len(s.Columns) > 0 {
			return &s, nil
		}
	}

	srcSchema, err := extractSourceSchemaHints(srcCfg)
	if err != nil {
		return nil, err
	}

	logical, err := sqlconv.ParseQueryToLogicalPlan(query)
	if err != nil {
		return nil, err
	}

	s, err := inferParquetSchemaFromLogicalPlan(logical, srcSchema)
	if err != nil {
		return nil, err
	}
	s.Version = 1
	s.QueryHash = queryHash

	if err := os.MkdirAll(filepath.Dir(cachePath), 0755); err != nil {
		return nil, fmt.Errorf("mkdir schema cache dir: %w", err)
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}
	if err := os.WriteFile(cachePath, b, 0644); err != nil {
		return nil, fmt.Errorf("write schema cache %s: %w", cachePath, err)
	}

	return s, nil
}

func defaultParquetSchemaCachePath(outputPath string) string {
	base := strings.TrimSpace(filepath.Base(outputPath))
	if base == "" || base == "." || base == string(os.PathSeparator) {
		base = "parquet"
	}
	dir := filepath.Dir(outputPath)
	if dir == "." || dir == string(os.PathSeparator) {
		dir = os.TempDir()
	}
	return filepath.Join(dir, base+".schema.json")
}

func ParseParquetSinkConfig(cfg map[string]interface{}) (*ParquetSinkConfig, error) {
	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parquet sink config: %w", err)
	}
	var out ParquetSinkConfig
	if err := yaml.Unmarshal(yamlBytes, &out); err != nil {
		return nil, fmt.Errorf("failed to parse parquet sink config: %w", err)
	}
	if strings.TrimSpace(out.Path) == "" {
		return nil, fmt.Errorf("parquet sink path is required")
	}
	return &out, nil
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
		out.Port = 8080 // Default port
	}
	if strings.TrimSpace(out.Path) == "" {
		out.Path = "/snapshot" // Default path
	}
	return &out, nil
}

func extractSourceSchemaHints(src SourceConfig) (map[string]string, error) {
	// Supported sources with schema hints: http/chain.
	schema := map[string]string{}

	switch src.Type {
	case "http":
		var c HTTPSourceConfig
		if err := decodeTo(src.Config, &c); err != nil {
			return nil, err
		}
		for k, v := range c.Schema {
			schema[k] = v
		}
	case "chain":
		var c ChainSourceConfig
		if err := decodeTo(src.Config, &c); err != nil {
			return nil, err
		}
		for _, s := range c.Sources {
			m, err := extractSourceSchemaHints(s)
			if err != nil {
				return nil, err
			}
			for k, v := range m {
				schema[k] = v
			}
		}
	default:
		// Unknown source type: return empty hints.
	}

	return schema, nil
}

func decodeTo(in map[string]interface{}, out any) error {
	yamlBytes, err := yaml.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := yaml.Unmarshal(yamlBytes, out); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	return nil
}

// DecodeTo parses a generic config map into a typed struct.
func DecodeTo(in map[string]interface{}, out any) error {
	return decodeTo(in, out)
}

func inferParquetSchemaFromLogicalPlan(node any, sourceSchema map[string]string) (*ParquetSchema, error) {
	// Walk the logical plan to determine output columns.
	cols := make([]ParquetColumn, 0, 8)

	addCol := func(name, typ string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		for _, c := range cols {
			if c.Name == name {
				return
			}
		}
		cols = append(cols, ParquetColumn{Name: name, Type: typ})
	}

	// Helper: map source type hint to parquet type.
	inferTypeFromHint := func(col string) string {
		h := strings.ToLower(strings.TrimSpace(sourceSchema[col]))
		switch h {
		case "int":
			return "int64"
		case "float":
			return "float64"
		case "string":
			return "string"
		case "bool":
			return "string" // ParquetSchema version 1 uses string/int64/float64
		case "timestamp":
			return "int64"
		case "json":
			return "string"
		default:
			return "string"
		}
	}

	addSourceSchemaCols := func() {
		keys := make([]string, 0, len(sourceSchema))
		for k := range sourceSchema {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			addCol(k, inferTypeFromHint(k))
		}
	}

	// Prefer concrete logical plan types for schema inference.
	switch n := node.(type) {
	case *ir.LogicalGroupAgg:
		for _, k := range n.Keys {
			addCol(k, inferTypeFromHint(k))
		}
		if len(n.Aggs) > 0 {
			for _, a := range n.Aggs {
				switch strings.ToUpper(strings.TrimSpace(a.Name)) {
				case "SUM":
					addCol("agg_delta", "float64")
				case "COUNT":
					addCol("count_delta", "int64")
				case "AVG":
					addCol("avg_delta", "float64")
				case "MIN":
					addCol("min", "string")
				case "MAX":
					addCol("max", "string")
				}
			}
		} else {
			switch strings.ToUpper(strings.TrimSpace(n.AggName)) {
			case "SUM":
				addCol("agg_delta", "float64")
			case "COUNT":
				addCol("count_delta", "int64")
			case "AVG":
				addCol("avg_delta", "float64")
			case "MIN":
				addCol("min", "string")
			case "MAX":
				addCol("max", "string")
			default:
				return nil, fmt.Errorf("unsupported aggregate for parquet schema: %s", n.AggName)
			}
		}
		addCol("__count", "int64")
		return &ParquetSchema{Columns: cols}, nil
	case *ir.LogicalWindowAgg:
		// Window columns are always present in WindowAggOp outputs.
		addCol("__window_start", "int64")
		addCol("__window_end", "int64")
		for _, k := range n.PartitionBy {
			addCol(k, inferTypeFromHint(k))
		}
		switch strings.ToUpper(strings.TrimSpace(n.AggName)) {
		case "SUM":
			addCol("agg_delta", "float64")
		case "COUNT":
			addCol("count_delta", "int64")
		case "AVG":
			addCol("avg_delta", "float64")
		case "MIN":
			addCol("min", "string")
		case "MAX":
			addCol("max", "string")
		default:
			return nil, fmt.Errorf("unsupported window aggregate for parquet schema: %s", n.AggName)
		}
		// Session windows can emit agg_result tuples via session diff logic.
		// Store it as float64 to keep schema stable.
		addCol("agg_result", "float64")
		addCol("__count", "int64")
		return &ParquetSchema{Columns: cols}, nil
	case *ir.LogicalProject:
		for _, k := range n.Columns {
			addCol(k, inferTypeFromHint(k))
		}
		for _, e := range n.Exprs {
			addCol(e.As, "string")
		}
		addCol("__count", "int64")
	case *ir.LogicalView:
		return inferParquetSchemaFromLogicalPlan(n.Input, sourceSchema)
	case *ir.LogicalFilter:
		return inferParquetSchemaFromLogicalPlan(n.Input, sourceSchema)
	case *ir.LogicalSort:
		return inferParquetSchemaFromLogicalPlan(n.Input, sourceSchema)
	case *ir.LogicalLimit:
		return inferParquetSchemaFromLogicalPlan(n.Input, sourceSchema)
	case *ir.LogicalWith:
		return inferParquetSchemaFromLogicalPlan(n.Body, sourceSchema)
	case *ir.LogicalScan:
		addSourceSchemaCols()
		addCol("__count", "int64")
	default:
		addSourceSchemaCols()
		addCol("__count", "int64")
	}

	// Keep deterministic order.
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
	return &ParquetSchema{Columns: cols}, nil
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
