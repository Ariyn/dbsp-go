package op

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/metrics"
)

type operatorProfileConfig struct {
	enabled bool
	every   int
}

var (
	operatorProfileConfigOnce sync.Once
	operatorProfileCfg        operatorProfileConfig
)

type operatorApplyProfile struct {
	label                 string
	enabled               bool
	reportEvery           int
	windowApplies         int
	totalApplies          int
	windowInRows          int64
	windowOutRows         int64
	windowDistinctOutRows int64
	windowAppendHits      int64
	windowAppendMisses    int64
}

func currentOperatorProfileConfig() operatorProfileConfig {
	operatorProfileConfigOnce.Do(func() {
		operatorProfileCfg = loadOperatorProfileConfigFromEnv(os.Getenv)
	})
	return operatorProfileCfg
}

func loadOperatorProfileConfigFromEnv(getenv func(string) string) operatorProfileConfig {
	if strings.TrimSpace(getenv("DBSP_OPERATOR_PROFILE")) == "" {
		return operatorProfileConfig{}
	}
	every := 20
	if raw := strings.TrimSpace(getenv("DBSP_OPERATOR_PROFILE_EVERY")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			every = parsed
		}
	}
	return operatorProfileConfig{enabled: true, every: every}
}

func newOperatorApplyProfile(label string) operatorApplyProfile {
	cfg := currentOperatorProfileConfig()
	return operatorApplyProfile{
		label:       label,
		enabled:     cfg.enabled,
		reportEvery: cfg.every,
	}
}

func (p *operatorApplyProfile) observeBatch(inRows int, out types.Batch, appendHits, appendMisses int) {
	distinctOutRows := distinctTupleCount(out)
	metrics.ObserveOperatorBatch(p.label, inRows, len(out), distinctOutRows, appendHits, appendMisses)
	if p == nil || !p.enabled {
		return
	}
	p.windowApplies++
	p.totalApplies++
	p.windowInRows += int64(inRows)
	p.windowOutRows += int64(len(out))
	p.windowDistinctOutRows += int64(distinctOutRows)
	p.windowAppendHits += int64(appendHits)
	p.windowAppendMisses += int64(appendMisses)
	if p.windowApplies%p.reportEvery == 0 {
		p.printWindowSummary()
		p.resetWindow()
	}
}

func (p *operatorApplyProfile) printWindowSummary() {
	if p.windowApplies == 0 {
		return
	}
	repeatedOutRows := p.windowOutRows - p.windowDistinctOutRows
	appendTotal := p.windowAppendHits + p.windowAppendMisses
	appendHitPct := 0.0
	if appendTotal > 0 {
		appendHitPct = float64(p.windowAppendHits) * 100 / float64(appendTotal)
	}
	amplification := 0.0
	if p.windowInRows > 0 {
		amplification = float64(p.windowOutRows) / float64(p.windowInRows)
	}
	fmt.Printf(
		"OP profile label=%s applies=%d total_applies=%d in_rows=%d out_rows=%d distinct_out_rows=%d repeated_out_rows=%d amplification=%.2f append_hits=%d append_misses=%d append_hit_pct=%.2f\n",
		p.label,
		p.windowApplies,
		p.totalApplies,
		p.windowInRows,
		p.windowOutRows,
		p.windowDistinctOutRows,
		repeatedOutRows,
		amplification,
		p.windowAppendHits,
		p.windowAppendMisses,
		appendHitPct,
	)
}

func (p *operatorApplyProfile) resetWindow() {
	p.windowApplies = 0
	p.windowInRows = 0
	p.windowOutRows = 0
	p.windowDistinctOutRows = 0
	p.windowAppendHits = 0
	p.windowAppendMisses = 0
}

func distinctTupleCount(batch types.Batch) int {
	if len(batch) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, len(batch))
	for _, td := range batch {
		if td.Tuple == nil {
			continue
		}
		seen[stableTupleKeyCanonical(td.Tuple)] = struct{}{}
	}
	return len(seen)
}
