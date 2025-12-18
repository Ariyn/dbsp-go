package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type FileSink struct {
	path           string
	format         string
	file           *os.File
	encoder        *json.Encoder // for json (JSON Lines)
	writer         *csv.Writer   // for csv
	headers        []string      // for csv
	needsCSVHeader bool
	mu             sync.Mutex
}

func NewFileSink(config map[string]interface{}) (*FileSink, error) {
	// Parse config
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var fileConfig FileSinkConfig
	if err := yaml.Unmarshal(yamlBytes, &fileConfig); err != nil {
		return nil, fmt.Errorf("failed to parse file sink config: %w", err)
	}

	if fileConfig.Path == "" {
		return nil, fmt.Errorf("file path is required")
	}
	if fileConfig.Format == "" {
		fileConfig.Format = "json"
	}

	// Open file for read/write so we can detect existing CSV header.
	f, err := os.OpenFile(fileConfig.Path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileConfig.Path, err)
	}

	sink := &FileSink{
		path:   fileConfig.Path,
		format: fileConfig.Format,
		file:   f,
	}

	if sink.format == "json" {
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to seek file %s: %w", fileConfig.Path, err)
		}
		sink.encoder = json.NewEncoder(f)
	} else if sink.format == "csv" {
		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to stat file %s: %w", fileConfig.Path, err)
		}

		// If file already has content, read its header so we keep column order stable.
		if stat.Size() > 0 {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("failed to seek file %s: %w", fileConfig.Path, err)
			}
			r := csv.NewReader(f)
			record, err := r.Read()
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("failed to read csv header from %s: %w", fileConfig.Path, err)
			}
			// Header is expected to end with __count; keep everything else as tuple columns.
			for _, col := range record {
				if col == "__count" {
					continue
				}
				sink.headers = append(sink.headers, col)
			}
			sink.needsCSVHeader = false
		} else {
			sink.needsCSVHeader = true
		}

		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed to seek file %s: %w", fileConfig.Path, err)
		}
		sink.writer = csv.NewWriter(f)
	} else {
		f.Close()
		return nil, fmt.Errorf("unsupported format: %s", sink.format)
	}

	return sink, nil
}

func (s *FileSink) WriteBatch(batch types.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	if s.format == "json" {
		// JSON Lines: one TupleDelta per line.
		for _, td := range batch {
			if err := s.encoder.Encode(td); err != nil {
				return err
			}
		}
		return nil
	} else if s.format == "csv" {
		// Initialize headers if needed.
		if s.headers == nil {
			firstTuple := batch[0].Tuple
			keys := make([]string, 0, len(firstTuple))
			for k := range firstTuple {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			s.headers = keys
		}

		if s.needsCSVHeader {
			headerRow := append([]string(nil), s.headers...)
			headerRow = append(headerRow, "__count")
			if err := s.writer.Write(headerRow); err != nil {
				return err
			}
			s.needsCSVHeader = false
		}

		for _, td := range batch {
			row := make([]string, 0, len(s.headers)+1)
			for _, key := range s.headers {
				val, ok := td.Tuple[key]
				if !ok || val == nil {
					row = append(row, "")
					continue
				}
				row = append(row, fmt.Sprintf("%v", val))
			}
			row = append(row, strconv.FormatInt(td.Count, 10))
			if err := s.writer.Write(row); err != nil {
				return err
			}
		}
		s.writer.Flush()
		return s.writer.Error()
	}

	return nil
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.Flush()
	}
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}
