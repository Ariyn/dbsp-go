package sqlconv

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ariyn/dbsp/internal/dbsp/op"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

func TestDuckDBTPCHOrders_SumTotalPriceByStatus(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	ordersCSV := filepath.Join(tmpDir, "orders.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, `INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01);`)

	// 2) Export a minimal Orders dataset as CSV (only columns we need)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderstatus AS o_orderstatus,
                CAST(o_totalprice AS DOUBLE) AS o_totalprice
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Compute expected result in DuckDB and export as CSV
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderstatus,
                CAST(SUM(o_totalprice) AS DOUBLE) AS sum_totalprice
            FROM orders
            GROUP BY o_orderstatus
            ORDER BY o_orderstatus
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedStatusToSum(t, expectedCSV)
	inputBatch := mustReadOrdersBatch(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT o_orderstatus, SUM(o_totalprice) FROM orders GROUP BY o_orderstatus`
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	g, ok := extractGroupAggOp(root.Op)
	if !ok {
		t.Fatalf("expected GroupAggOp in compiled plan, got %T", root.Op)
	}

	_, err = op.Execute(root, inputBatch)
	if err != nil {
		t.Fatalf("DBSP execute failed: %v", err)
	}

	got := g.State()
	if len(got) != len(expected) {
		t.Fatalf("group count mismatch: got %d, expected %d", len(got), len(expected))
	}

	for k, want := range expected {
		v, ok := got[k]
		if !ok {
			t.Fatalf("missing group key %q", k)
		}
		gotF, ok := toFloat64(v)
		if !ok {
			t.Fatalf("unexpected aggregate type for key %q: %T", k, v)
		}
		if !floatAlmostEqual(gotF, want, 1e-9, 1e-6) {
			t.Fatalf("sum mismatch for key %q: got %v, expected %v", k, gotF, want)
		}
	}
}

func duckdbBinaryPath(t *testing.T) (string, bool) {
	t.Helper()

	if p := strings.TrimSpace(os.Getenv("DUCKDB_PATH")); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, true
		}
		t.Fatalf("DUCKDB_PATH is set but not usable: %q", p)
	}

	p, err := exec.LookPath("duckdb")
	if err != nil {
		t.Skip("duckdb binary not found in PATH (set DUCKDB_PATH to override)")
		return "", false
	}
	return p, true
}

func mustRunDuckDB(t *testing.T, ctx context.Context, duckdbPath, dbPath, sql string) {
	t.Helper()

	cmd := exec.CommandContext(ctx, duckdbPath, dbPath, "-c", sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("duckdb command timed out: %s", sql)
	}
	t.Fatalf("duckdb command failed: %v\nSQL: %s\nSTDOUT: %s\nSTDERR: %s", err, sql, stdout.String(), stderr.String())
}

func mustReadOrdersBatch(t *testing.T, csvPath string) types.Batch {
	t.Helper()

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open orders csv: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		t.Fatalf("read header: %v", err)
	}

	colIndex := make(map[string]int, len(header))
	for i, name := range header {
		colIndex[name] = i
	}

	statusIdx, ok := colIndex["o_orderstatus"]
	if !ok {
		t.Fatalf("orders csv missing column o_orderstatus; header=%v", header)
	}
	priceIdx, ok := colIndex["o_totalprice"]
	if !ok {
		t.Fatalf("orders csv missing column o_totalprice; header=%v", header)
	}

	var batch types.Batch
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}

		status := rec[statusIdx]
		priceStr := rec[priceIdx]
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			t.Fatalf("parse price %q: %v", priceStr, err)
		}

		tup := types.Tuple{
			"o_orderstatus": status,
			"o_totalprice":  price,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadExpectedStatusToSum(t *testing.T, csvPath string) map[any]float64 {
	t.Helper()

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open expected csv: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	colIndex := make(map[string]int, len(header))
	for i, name := range header {
		colIndex[name] = i
	}

	statusIdx, ok := colIndex["o_orderstatus"]
	if !ok {
		t.Fatalf("expected csv missing column o_orderstatus; header=%v", header)
	}
	sumIdx, ok := colIndex["sum_totalprice"]
	if !ok {
		t.Fatalf("expected csv missing column sum_totalprice; header=%v", header)
	}

	out := make(map[any]float64)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		status := rec[statusIdx]
		sumStr := rec[sumIdx]
		sum, err := strconv.ParseFloat(sumStr, 64)
		if err != nil {
			t.Fatalf("parse sum %q: %v", sumStr, err)
		}
		out[status] = sum
	}
	return out
}

func extractGroupAggOp(root op.Operator) (*op.GroupAggOp, bool) {
	if g, ok := root.(*op.GroupAggOp); ok {
		return g, true
	}
	c, ok := root.(*op.ChainedOp)
	if !ok {
		return nil, false
	}
	for _, o := range c.Ops {
		if g, ok := o.(*op.GroupAggOp); ok {
			return g, true
		}
	}
	return nil, false
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	default:
		return 0, false
	}
}

func floatAlmostEqual(a, b, relTol, absTol float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	if diff <= absTol {
		return true
	}
	maxAB := a
	if maxAB < 0 {
		maxAB = -maxAB
	}
	bb := b
	if bb < 0 {
		bb = -bb
	}
	if bb > maxAB {
		maxAB = bb
	}
	if maxAB == 0 {
		return diff == 0
	}
	return diff/maxAB <= relTol
}
