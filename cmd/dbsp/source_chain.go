package main

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type ChainSourceConfig struct {
	Sources []SourceConfig `yaml:"sources"`
}

type ChainSource struct {
	sources []Source
	current int
}

func NewChainSource(config map[string]interface{}) (*ChainSource, error) {
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var chainConfig ChainSourceConfig
	if err := yaml.Unmarshal(yamlBytes, &chainConfig); err != nil {
		return nil, fmt.Errorf("failed to parse chain config: %w", err)
	}

	var sources []Source
	for _, srcConfig := range chainConfig.Sources {
		var s Source
		var err error
		switch srcConfig.Type {
		case "csv":
			s, err = NewCSVSource(srcConfig.Config)
		case "http":
			s, err = NewHTTPSource(srcConfig.Config)
		default:
			err = fmt.Errorf("unsupported source type in chain: %s", srcConfig.Type)
		}
		if err != nil {
			// Clean up already created sources
			for _, created := range sources {
				created.Close()
			}
			return nil, err
		}
		sources = append(sources, s)
	}

	return &ChainSource{
		sources: sources,
		current: 0,
	}, nil
}

func (s *ChainSource) NextBatch() (types.Batch, error) {
	for s.current < len(s.sources) {
		batch, err := s.sources[s.current].NextBatch()
		if err != nil {
			return nil, err
		}
		if batch != nil {
			return batch, nil
		}
		// Current source exhausted, move to next
		s.current++
	}
	return nil, nil
}

func (s *ChainSource) Close() error {
	var lastErr error
	for _, src := range s.sources {
		if err := src.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
