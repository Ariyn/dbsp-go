package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

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

func inferOrLoadParquetSchema(query string, srcCfg SourceConfig, sinkCfg map[string]interface{}) (*ParquetSchema, error) {
	cfg, err := parseParquetSinkConfig(sinkCfg)
	if err != nil {
		return nil, err
	}

	queryHash := hashString(query)
	cachePath := strings.TrimSpace(cfg.SchemaCachePath)
	if cachePath == "" {
		// Default: next to the Parquet path prefix.
		cachePath = cfg.Path + ".schema.json"
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
			return nil, fmt.Errorf("schema cache query_hash mismatch (cache=%s current=%s)", s.QueryHash, queryHash)
		}
		if len(s.Columns) == 0 {
			return nil, fmt.Errorf("schema cache has no columns")
		}
		return &s, nil
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

func parseParquetSinkConfig(cfg map[string]interface{}) (*ParquetSinkConfig, error) {
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

func extractSourceSchemaHints(src SourceConfig) (map[string]string, error) {
	// Supported sources with schema hints: csv/http/chain.
	schema := map[string]string{}

	switch src.Type {
	case "csv":
		var c CSVSourceConfig
		if err := decodeTo(src.Config, &c); err != nil {
			return nil, err
		}
		for k, v := range c.Schema {
			schema[k] = v
		}
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
		default:
			return "string"
		}
	}

	// Recognize logical plan types without importing ir directly here.
	// We key off concrete type names since sqlconv.ParseQueryToLogicalPlan returns ir.LogicalNode.
	typeName := fmt.Sprintf("%T", node)

	// GroupAgg
	if strings.HasSuffix(typeName, ".LogicalGroupAgg") {
		// Use JSON marshal/unmarshal to avoid depending on internal struct fields here.
		// We only need Keys and AggName/Aggs.
		b, _ := json.Marshal(node)
		var tmp struct {
			Keys    []string                     `json:"Keys"`
			Aggs    []struct{ Name, Col string } `json:"Aggs"`
			AggName string                       `json:"AggName"`
		}
		_ = json.Unmarshal(b, &tmp)

		for _, k := range tmp.Keys {
			addCol(k, inferTypeFromHint(k))
		}

		if len(tmp.Aggs) > 0 {
			for _, a := range tmp.Aggs {
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
				default:
					// ignore
				}
			}
		} else {
			switch strings.ToUpper(strings.TrimSpace(tmp.AggName)) {
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
				return nil, fmt.Errorf("unsupported aggregate for parquet schema: %s", tmp.AggName)
			}
		}

		addCol("__count", "int64")
		return &ParquetSchema{Columns: cols}, nil
	}

	// WindowAgg (event-time window GROUP BY).
	if strings.HasSuffix(typeName, ".LogicalWindowAgg") {
		b, _ := json.Marshal(node)
		var tmp struct {
			AggName        string    `json:"AggName"`
			PartitionBy    []string  `json:"PartitionBy"`
			TimeWindowSpec *struct{} `json:"TimeWindowSpec"`
		}
		_ = json.Unmarshal(b, &tmp)

		// Window columns are always present in WindowAggOp outputs.
		addCol("__window_start", "int64")
		addCol("__window_end", "int64")

		for _, k := range tmp.PartitionBy {
			addCol(k, inferTypeFromHint(k))
		}

		switch strings.ToUpper(strings.TrimSpace(tmp.AggName)) {
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
			return nil, fmt.Errorf("unsupported window aggregate for parquet schema: %s", tmp.AggName)
		}

		// Session windows can emit agg_result tuples via session diff logic.
		// Store it as float64 to keep schema stable.
		addCol("agg_result", "float64")

		addCol("__count", "int64")
		return &ParquetSchema{Columns: cols}, nil
	}

	// Fallback: try to include just __count.
	addCol("__count", "int64")

	// Keep deterministic order.
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
	return &ParquetSchema{Columns: cols}, nil
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
