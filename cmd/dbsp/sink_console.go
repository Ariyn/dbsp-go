package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ariyn/dbsp/internal/dbsp/types"
)

type ConsoleSink struct {
	format string
}

func NewConsoleSink(config map[string]interface{}) (*ConsoleSink, error) {
	format := "json"
	if f, ok := config["format"].(string); ok {
		format = f
	}
	return &ConsoleSink{format: format}, nil
}

func (s *ConsoleSink) WriteBatch(batch types.Batch) error {
	if len(batch) == 0 {
		return nil
	}

	if s.format == "json" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(batch)
	}

	// Simple text format
	for _, td := range batch {
		fmt.Printf("%+v (Count: %d)\n", td.Tuple, td.Count)
	}
	return nil
}

func (s *ConsoleSink) Close() error {
	return nil
}
