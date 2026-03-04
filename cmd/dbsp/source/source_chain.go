package source

import (
	"fmt"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/cmd/dbsp/provider"
	"github.com/ariyn/dbsp/internal/dbsp/types"
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

func NewChainSource(chainConfig config.ChainSourceConfig) (*ChainSource, error) {
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
		var s provider.Source
		var err error
		switch srcConfig.Type {
		case "http":
			var httpCfg config.HTTPSourceConfig
			if err := config.DecodeTo(srcConfig.Config, &httpCfg); err != nil {
				return nil, fmt.Errorf("failed to decode http source config in chain: %w", err)
			}
			s, err = NewHTTPSource(httpCfg)
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
