package main

import (
	"fmt"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type ChainSourceConfig struct {
	OnError string         `yaml:"on_error"`
	Sources []SourceConfig `yaml:"sources"`
}

type ChainSource struct {
	sources []Source
	current int
	onError chainOnErrorPolicy
}

type chainOnErrorPolicy string

const (
	chainOnErrorStop chainOnErrorPolicy = "stop"
	chainOnErrorSkip chainOnErrorPolicy = "skip"
)

type sourceFactory func(config map[string]interface{}) (Source, error)

func NewChainSource(config map[string]interface{}) (*ChainSource, error) {
	factories := map[string]sourceFactory{
		"csv":  func(cfg map[string]interface{}) (Source, error) { return NewCSVSource(cfg) },
		"http": func(cfg map[string]interface{}) (Source, error) { return NewHTTPSource(cfg) },
	}
	return newChainSourceWithFactories(config, factories)
}

func newChainSourceWithFactories(config map[string]interface{}, factories map[string]sourceFactory) (*ChainSource, error) {
	yamlBytes, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var chainConfig ChainSourceConfig
	if err := yaml.Unmarshal(yamlBytes, &chainConfig); err != nil {
		return nil, fmt.Errorf("failed to parse chain config: %w", err)
	}

	onError := chainOnErrorPolicy(chainConfig.OnError)
	if onError == "" {
		onError = chainOnErrorStop
	}
	switch onError {
	case chainOnErrorStop, chainOnErrorSkip:
		// ok
	default:
		return nil, fmt.Errorf("invalid chain on_error policy: %s", chainConfig.OnError)
	}

	var sources []Source
	for _, srcConfig := range chainConfig.Sources {
		factory, ok := factories[srcConfig.Type]
		if !ok {
			err = fmt.Errorf("unsupported source type in chain: %s", srcConfig.Type)
			// Clean up already created sources
			for _, created := range sources {
				created.Close()
			}
			return nil, err
		}

		s, err := factory(srcConfig.Config)
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
		onError: onError,
	}, nil
}

func (s *ChainSource) NextBatch() (types.Batch, error) {
	for s.current < len(s.sources) {
		batch, err := s.sources[s.current].NextBatch()
		if err != nil {
			switch s.onError {
			case chainOnErrorStop:
				return nil, err
			case chainOnErrorSkip:
				_ = s.sources[s.current].Close()
				s.current++
				continue
			default:
				return nil, err
			}
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
