package main

import (
	"errors"
	"testing"

	"github.com/ariyn/dbsp/cmd/dbsp/config"
	"github.com/ariyn/dbsp/internal/dbsp/op"
)

func TestPreflightPartitionQueryBuild_SkipsWhenPartitionDisabled(t *testing.T) {
	orig := compileIncrementalQuery
	defer func() { compileIncrementalQuery = orig }()

	called := false
	compileIncrementalQuery = func(query string) (*op.Node, error) {
		called = true
		return nil, nil
	}

	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Transform.Type = "sql"
	cfg.Pipeline.Transform.Query = "SELECT 1"
	cfg.Pipeline.Partition.Enabled = false

	if err := preflightPartitionQueryBuild(cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if called {
		t.Fatal("expected compiler not to be called when partition is disabled")
	}
}

func TestPreflightPartitionQueryBuild_FailsOnCompileError(t *testing.T) {
	orig := compileIncrementalQuery
	defer func() { compileIncrementalQuery = orig }()

	compileIncrementalQuery = func(query string) (*op.Node, error) {
		return nil, errors.New("syntax error")
	}

	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Transform.Type = "sql"
	cfg.Pipeline.Transform.Query = "SELECT"
	cfg.Pipeline.Partition.Enabled = true

	err := preflightPartitionQueryBuild(cfg)
	if err == nil {
		t.Fatal("expected preflight error, got nil")
	}
}

func TestPreflightPartitionQueryBuild_PassesOnCompileSuccess(t *testing.T) {
	orig := compileIncrementalQuery
	defer func() { compileIncrementalQuery = orig }()

	compileIncrementalQuery = func(query string) (*op.Node, error) {
		return &op.Node{}, nil
	}

	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Transform.Type = "sql"
	cfg.Pipeline.Transform.Query = "SELECT 1"
	cfg.Pipeline.Partition.Enabled = true

	if err := preflightPartitionQueryBuild(cfg); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestPreflightPartitionQueryBuild_ErrsOnNilConfig(t *testing.T) {
	if err := preflightPartitionQueryBuild(nil); err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

func TestPreflightPartitionQueryBuild_ErrsOnNonSQLTransform(t *testing.T) {
	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Transform.Type = "python"
	cfg.Pipeline.Transform.Query = "SELECT 1"
	cfg.Pipeline.Partition.Enabled = true

	if err := preflightPartitionQueryBuild(cfg); err == nil {
		t.Fatal("expected error for non-sql transform, got nil")
	}
}

func TestPreflightPartitionQueryBuild_ErrsOnEmptyQuery(t *testing.T) {
	cfg := &config.PipelineConfig{}
	cfg.Pipeline.Transform.Type = "sql"
	cfg.Pipeline.Transform.Query = "   "
	cfg.Pipeline.Partition.Enabled = true

	if err := preflightPartitionQueryBuild(cfg); err == nil {
		t.Fatal("expected error for empty query, got nil")
	}
}
