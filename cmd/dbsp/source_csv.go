package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type CSVSource struct {
	file    *os.File
	reader  *csv.Reader
	schema  map[string]string
	headers []string
	done    bool
}

func NewCSVSource(config map[string]interface{}) (*CSVSource, error) {
	// Parse config
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var csvConfig CSVSourceConfig
	if err := yaml.Unmarshal(yamlBytes, &csvConfig); err != nil {
		return nil, fmt.Errorf("failed to parse csv config: %w", err)
	}

	file, err := os.Open(csvConfig.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", csvConfig.Path, err)
	}

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	return &CSVSource{
		file:    file,
		reader:  reader,
		schema:  csvConfig.Schema,
		headers: headers,
		done:    false,
	}, nil
}

func (s *CSVSource) NextBatch() (types.Batch, error) {
	if s.done {
		return nil, nil
	}

	// For this implementation, we read the entire file as one batch.
	// In a real streaming scenario, we might read a fixed number of rows.
	var batch types.Batch

	for {
		record, err := s.reader.Read()
		if err == io.EOF {
			s.done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading csv record: %w", err)
		}

		tuple := make(types.Tuple)
		for i, value := range record {
			if i >= len(s.headers) {
				continue
			}
			colName := s.headers[i]
			colType, ok := s.schema[colName]
			if !ok {
				// Default to string if not in schema
				tuple[colName] = value
				continue
			}

			parsedValue, err := parseValue(value, colType)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value '%s' for column '%s' as %s: %w", value, colName, colType, err)
			}
			tuple[colName] = parsedValue
		}

		// Assume all records in CSV are insertions (+1)
		batch = append(batch, types.TupleDelta{
			Tuple: tuple,
			Count: 1,
		})
	}

	if len(batch) == 0 {
		return nil, nil
	}

	return batch, nil
}

func (s *CSVSource) Close() error {
	return s.file.Close()
}

func parseValue(value string, colType string) (any, error) {
	switch colType {
	case "int":
		return strconv.Atoi(value)
	case "float":
		return strconv.ParseFloat(value, 64)
	case "string":
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", colType)
	}
}
