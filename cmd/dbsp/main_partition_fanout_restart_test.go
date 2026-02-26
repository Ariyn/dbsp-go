package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
	"github.com/ariyn/dbsp/internal/dbsp/wal"
)

type fanoutPassthroughOp struct{}

func (fanoutPassthroughOp) Apply(batch types.Batch) (types.Batch, error) { return batch, nil }

func writeCSVForFanoutTest(t *testing.T, path string, lines []string) {
	t.Helper()
	content := "region,amount\n"
	for _, line := range lines {
		content += line + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
}

func openWALAndMaxSeq(t *testing.T, path string) int64 {
	t.Helper()
	w, err := wal.NewSQLiteWAL(path)
	if err != nil {
		t.Fatalf("open wal %s: %v", path, err)
	}
	defer w.Close()

	seq, err := w.MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq(%s): %v", path, err)
	}
	return seq
}

func TestRunPartitionFanout_Restart_MultiPartitionWALProgressesIndependently(t *testing.T) {
	origCompile := compileIncrementalQuery
	defer func() { compileIncrementalQuery = origCompile }()
	compileIncrementalQuery = func(string) (*op.Node, error) {
		return &op.Node{Op: fanoutPassthroughOp{}}, nil
	}

	tmp := t.TempDir()
	csvPath := filepath.Join(tmp, "input.csv")
	walBasePath := filepath.Join(tmp, "wal.db")
	sinkBasePath := filepath.Join(tmp, "out.jsonl")

	writeCSVForFanoutTest(t, csvPath, []string{"ap,10", "eu,20"})

	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Source.Type = "csv"
	cfg.Pipeline.Source.Config = map[string]interface{}{
		"path": csvPath,
		"schema": map[string]interface{}{
			"region": "string",
			"amount": "int",
		},
	}
	cfg.Pipeline.Transform.Type = "sql"
	cfg.Pipeline.Transform.Query = "SELECT region, amount FROM sales"
	cfg.Pipeline.Sink.Type = "file"
	cfg.Pipeline.Sink.Config = map[string]interface{}{
		"path":   sinkBasePath,
		"format": "json",
	}
	cfg.Pipeline.Partition.Enabled = true
	cfg.Pipeline.Partition.Keys = []string{"region"}
	cfg.Pipeline.WAL.Enabled = true
	cfg.Pipeline.WAL.Path = walBasePath
	cfg.Pipeline.WAL.CheckpointEveryBatches = 1

	if err := runPartitionFanout(context.Background(), cfg); err != nil {
		t.Fatalf("runPartitionFanout(first): %v", err)
	}

	apValues := map[string]string{"region": "ap"}
	euValues := map[string]string{"region": "eu"}
	apWalPath := config.BuildHivePartitionPath(walBasePath, cfg.Pipeline.Partition.Keys, apValues)
	euWalPath := config.BuildHivePartitionPath(walBasePath, cfg.Pipeline.Partition.Keys, euValues)

	if got := openWALAndMaxSeq(t, apWalPath); got != 1 {
		t.Fatalf("expected ap max seq=1 after first run, got %d", got)
	}
	if got := openWALAndMaxSeq(t, euWalPath); got != 1 {
		t.Fatalf("expected eu max seq=1 after first run, got %d", got)
	}

	apSinkPath := config.BuildHivePartitionPath(sinkBasePath, cfg.Pipeline.Partition.Keys, apValues)
	euSinkPath := config.BuildHivePartitionPath(sinkBasePath, cfg.Pipeline.Partition.Keys, euValues)
	apSinkStat1, err := os.Stat(apSinkPath)
	if err != nil {
		t.Fatalf("stat ap sink first run: %v", err)
	}
	euSinkStat1, err := os.Stat(euSinkPath)
	if err != nil {
		t.Fatalf("stat eu sink first run: %v", err)
	}

	writeCSVForFanoutTest(t, csvPath, []string{"ap,30"})
	if err := runPartitionFanout(context.Background(), cfg); err != nil {
		t.Fatalf("runPartitionFanout(second): %v", err)
	}

	if got := openWALAndMaxSeq(t, apWalPath); got != 2 {
		t.Fatalf("expected ap max seq=2 after second run, got %d", got)
	}
	if got := openWALAndMaxSeq(t, euWalPath); got != 1 {
		t.Fatalf("expected eu max seq=1 after second run, got %d", got)
	}

	apSinkStat2, err := os.Stat(apSinkPath)
	if err != nil {
		t.Fatalf("stat ap sink second run: %v", err)
	}
	euSinkStat2, err := os.Stat(euSinkPath)
	if err != nil {
		t.Fatalf("stat eu sink second run: %v", err)
	}
	if apSinkStat2.Size() <= apSinkStat1.Size() {
		t.Fatalf("expected ap sink file to grow after second run: before=%d after=%d", apSinkStat1.Size(), apSinkStat2.Size())
	}
	if euSinkStat2.Size() != euSinkStat1.Size() {
		t.Fatalf("expected eu sink file size unchanged on second run: before=%d after=%d", euSinkStat1.Size(), euSinkStat2.Size())
	}
}
