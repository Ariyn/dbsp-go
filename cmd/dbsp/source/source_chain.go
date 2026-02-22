package source

import (
	"fmt"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"gopkg.in/yaml.v3"
)

type ChainSource struct {
	sources []provider.Source
	current int
	onError chainOnErrorPolicy
}

type chainOnErrorPolicy string

const (
	chainOnErrorStop chainOnErrorPolicy = "stop"
	chainOnErrorSkip chainOnErrorPolicy = "skip"
)

type sourceFactory func(config map[string]interface{}) (provider.Source, error)

func NewChainSource(cfg map[string]interface{}) (*ChainSource, error) {
	factories := map[string]sourceFactory{
		"csv":  func(c map[string]interface{}) (provider.Source, error) { return NewCSVSource(c) },
		"http": func(c map[string]interface{}) (provider.Source, error) { return NewHTTPSource(c) },
	}
	return newChainSourceWithFactories(cfg, factories)
}

func newChainSourceWithFactories(cfg map[string]interface{}, factories map[string]sourceFactory) (*ChainSource, error) {
	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	var chainConfig config.ChainSourceConfig
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

	var sources []provider.Source
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
