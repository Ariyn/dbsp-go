package config

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

func ValidatePartitionConfig(cfg PartitionConfig, transformQuery string) error {
	if !cfg.Enabled {
		return nil
	}
	if len(cfg.Keys) == 0 {
		return fmt.Errorf("partition.keys is required when partition.enabled=true")
	}
	if len(cfg.Jobs) > 0 {
		return fmt.Errorf("partition.jobs is no longer supported; dynamic split is derived from incoming partition key values")
	}

	seen := map[string]struct{}{}
	for _, key := range cfg.Keys {
		k := strings.TrimSpace(key)
		if k == "" {
			return fmt.Errorf("partition.keys must not contain empty key")
		}
		if _, ok := seen[k]; ok {
			return fmt.Errorf("duplicate partition key: %s", k)
		}
		seen[k] = struct{}{}
	}

	if strings.TrimSpace(transformQuery) == "" {
		return fmt.Errorf("transform.query is required when partition.enabled=true")
	}

	return nil
}

func QueryContainsPartitionPredicate(query string, keys []string) bool {
	q := strings.ToUpper(query)
	if !strings.Contains(q, "WHERE") {
		return false
	}
	for _, key := range keys {
		k := strings.ToUpper(strings.TrimSpace(key))
		if k == "" {
			continue
		}
		if strings.Contains(q, " "+k+" =") || strings.Contains(q, "("+k+" =") || strings.Contains(q, " AND "+k+"=") || strings.Contains(q, " WHERE "+k+"=") {
			return true
		}
	}
	return false
}

func BuildHivePartitionPath(basePath string, keys []string, values map[string]string) string {
	base := strings.TrimSpace(basePath)
	if base == "" {
		return base
	}

	partDirs := make([]string, 0, len(keys))
	for _, key := range keys {
		partDirs = append(partDirs, fmt.Sprintf("%s=%s", sanitizeHivePathSegment(key), sanitizeHivePathSegment(values[key])))
	}

	// Keep filename for file-like paths, otherwise treat as prefix/directory.
	ext := strings.ToLower(filepath.Ext(base))
	switch ext {
	case ".json", ".jsonl", ".csv", ".parquet", ".db", ".sqlite", ".sqlite3":
		dir := filepath.Dir(base)
		file := filepath.Base(base)
		return filepath.Join(append([]string{dir}, append(partDirs, file)...)...)
	default:
		parts := append([]string{base}, partDirs...)
		return filepath.Join(parts...)
	}
}

func sanitizeHivePathSegment(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, string(filepath.Separator), "_")
	v = strings.ReplaceAll(v, "=", "_")
	v = strings.ReplaceAll(v, "..", "_")
	if v == "" {
		return "_"
	}
	return v
}

func PartitionSummary(values map[string]string, keys []string) string {
	if len(values) == 0 {
		return ""
	}
	if len(keys) == 0 {
		keys = make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return strings.Join(parts, ",")
}
