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
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

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

func TestDuckDBTPCHOrders_CountByStatus(t *testing.T) {
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
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export a minimal Orders dataset as CSV (only columns we need)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderstatus AS o_orderstatus
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Compute expected result in DuckDB and export as CSV
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderstatus,
                CAST(COUNT(o_orderstatus) AS BIGINT) AS cnt
            FROM orders
            GROUP BY o_orderstatus
            ORDER BY o_orderstatus
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedStatusToCount(t, expectedCSV)
	inputBatch := mustReadOrdersStatusBatch(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT o_orderstatus, COUNT(o_orderstatus) FROM orders GROUP BY o_orderstatus`
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
		gotI, ok := toInt64(v)
		if !ok {
			t.Fatalf("unexpected aggregate type for key %q: %T", k, v)
		}
		if gotI != want {
			t.Fatalf("count mismatch for key %q: got %v, expected %v", k, gotI, want)
		}
	}
}

func TestDuckDBTPCHOrders_FilteredSumTotalPriceByPriority(t *testing.T) {
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
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export a minimal Orders dataset as CSV (only columns we need)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderstatus AS o_orderstatus,
                o_orderpriority AS o_orderpriority,
                CAST(o_totalprice AS DOUBLE) AS o_totalprice
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Compute expected result in DuckDB and export as CSV
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                o_orderpriority,
                CAST(SUM(o_totalprice) AS DOUBLE) AS sum_totalprice
            FROM orders
            WHERE o_orderstatus = 'F'
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedPriorityToSum(t, expectedCSV)
	inputBatch := mustReadOrdersBatchWithPriority(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT o_orderpriority, SUM(o_totalprice) FROM orders WHERE o_orderstatus = 'F' GROUP BY o_orderpriority`
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

func TestDuckDBTPCHLineitem_SumQuantityByReturnFlag(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	lineitemCSV := filepath.Join(tmpDir, "lineitem.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export a minimal Lineitem dataset as CSV (only columns we need)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                l_returnflag AS l_returnflag,
                CAST(l_quantity AS DOUBLE) AS l_quantity
            FROM lineitem
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(lineitemCSV),
	))

	// 3) Compute expected result in DuckDB and export as CSV
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                l_returnflag,
                CAST(SUM(l_quantity) AS DOUBLE) AS sum_quantity
            FROM lineitem
            GROUP BY l_returnflag
            ORDER BY l_returnflag
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedReturnFlagToSumQuantity(t, expectedCSV)
	inputBatch := mustReadLineitemBatch(t, lineitemCSV)

	// 4) Run DBSP on the same input
	query := `SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY l_returnflag`
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

func TestDuckDBTPCHLineitem_FilteredSumQuantityByLineStatus(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	lineitemCSV := filepath.Join(tmpDir, "lineitem.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export a minimal Lineitem dataset as CSV (only columns we need)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                l_returnflag AS l_returnflag,
                l_linestatus AS l_linestatus,
                CAST(l_quantity AS DOUBLE) AS l_quantity
            FROM lineitem
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(lineitemCSV),
	))

	// 3) Compute expected result in DuckDB and export as CSV
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                l_linestatus,
                CAST(SUM(l_quantity) AS DOUBLE) AS sum_quantity
            FROM lineitem
            WHERE l_returnflag = 'R'
            GROUP BY l_linestatus
            ORDER BY l_linestatus
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedLineStatusToSumQuantity(t, expectedCSV)
	inputBatch := mustReadLineitemBatchWithLineStatus(t, lineitemCSV)

	// 4) Run DBSP on the same input
	query := `SELECT l_linestatus, SUM(l_quantity) FROM lineitem WHERE l_returnflag = 'R' GROUP BY l_linestatus`
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

func TestDuckDBTPCHJoin_CustomerNation_SumOrderTotalPrice(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	customerCSV := filepath.Join(tmpDir, "customer.csv")
	ordersCSV := filepath.Join(tmpDir, "orders.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export minimal customer/orders CSVs with column names matching DBSP join keys.
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(c_custkey AS BIGINT) AS "customer.c_custkey",
                CAST(c_nationkey AS BIGINT) AS "customer.c_nationkey"
            FROM customer
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(customerCSV),
	))

	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(o_custkey AS BIGINT) AS "orders.o_custkey",
                CAST(o_totalprice AS DOUBLE) AS "orders.o_totalprice"
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Expected result from DuckDB
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(customer.c_nationkey AS BIGINT) AS nationkey,
                CAST(SUM(orders.o_totalprice) AS DOUBLE) AS sum_totalprice
            FROM customer
            JOIN orders ON customer.c_custkey = orders.o_custkey
            GROUP BY customer.c_nationkey
            ORDER BY nationkey
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedNationKeyToSumTotalPrice(t, expectedCSV)
	customerBatch := mustReadCustomerBatchForJoin(t, customerCSV)
	ordersBatch := mustReadOrdersBatchForJoin(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT customer.c_nationkey, SUM(orders.o_totalprice)
		FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey
		GROUP BY customer.c_nationkey`
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	g, ok := extractGroupAggOp(root.Op)
	if !ok {
		t.Fatalf("expected GroupAggOp in compiled plan, got %T", root.Op)
	}

	// Execute in two steps to avoid expensive ΔR⋈ΔS cross-product work.
	_, err = op.ExecuteTick(root, map[string]types.Batch{"customer": customerBatch})
	if err != nil {
		t.Fatalf("DBSP execute failed: %v", err)
	}
	_, err = op.ExecuteTick(root, map[string]types.Batch{"orders": ordersBatch})
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
			t.Fatalf("missing group key %v", k)
		}
		gotF, ok := toFloat64(v)
		if !ok {
			t.Fatalf("unexpected aggregate type for key %v: %T", k, v)
		}
		if !floatAlmostEqual(gotF, want, 1e-9, 1e-6) {
			t.Fatalf("sum mismatch for key %v: got %v, expected %v", k, gotF, want)
		}
	}
}

func TestDuckDBTPCHJoin_FilteredOrdersStatus_ProjectedRows(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	customerCSV := filepath.Join(tmpDir, "customer.csv")
	ordersCSV := filepath.Join(tmpDir, "orders.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export minimal customer/orders CSVs with column names matching DBSP join keys.
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(c_custkey AS BIGINT) AS "customer.c_custkey",
                CAST(c_nationkey AS BIGINT) AS "customer.c_nationkey"
            FROM customer
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(customerCSV),
	))

	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(o_custkey AS BIGINT) AS "orders.o_custkey",
                o_orderstatus AS "orders.o_orderstatus"
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Expected (projection rows) from DuckDB
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(customer.c_nationkey AS BIGINT) AS nationkey,
                orders.o_orderstatus AS orderstatus
            FROM customer
            JOIN orders ON customer.c_custkey = orders.o_custkey
            WHERE orders.o_orderstatus = 'F'
            ORDER BY nationkey, orderstatus
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedJoinRowsMultiset(t, expectedCSV)
	customerBatch := mustReadCustomerBatchForJoin(t, customerCSV)
	ordersBatch := mustReadOrdersBatchForJoinWithStatus(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT customer.c_nationkey, orders.o_orderstatus
		FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey
		WHERE orders.o_orderstatus = 'F'`
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	// Execute in two steps to avoid expensive ΔR⋈ΔS cross-product work.
	var got map[string]int64
	out1, err := op.ExecuteTick(root, map[string]types.Batch{"customer": customerBatch})
	if err != nil {
		t.Fatalf("DBSP execute failed: %v", err)
	}
	out2, err := op.ExecuteTick(root, map[string]types.Batch{"orders": ordersBatch})
	if err != nil {
		t.Fatalf("DBSP execute failed: %v", err)
	}
	got = mergeMultisets(batchToJoinRowsMultiset(out1), batchToJoinRowsMultiset(out2))
	if !multisetEqual(got, expected) {
		t.Fatalf("join row multiset mismatch: got=%v expected=%v", got, expected)
	}
}

func TestDuckDBTPCHJoin_FilteredOrdersStatus_GroupSumByNation(t *testing.T) {
	duckdbPath, ok := duckdbBinaryPath(t)
	if !ok {
		return
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tpch.duckdb")
	customerCSV := filepath.Join(tmpDir, "customer.csv")
	ordersCSV := filepath.Join(tmpDir, "orders.csv")
	expectedCSV := filepath.Join(tmpDir, "expected.csv")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// 1) Generate TPC-H data (tiny scale for tests)
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(`INSTALL tpch; LOAD tpch; CALL dbgen(sf=%s);`, tpchScaleFactor(t)))

	// 2) Export minimal customer/orders CSVs with column names matching DBSP join keys.
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(c_custkey AS BIGINT) AS "customer.c_custkey",
                CAST(c_nationkey AS BIGINT) AS "customer.c_nationkey"
            FROM customer
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(customerCSV),
	))

	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(o_custkey AS BIGINT) AS "orders.o_custkey",
                o_orderstatus AS "orders.o_orderstatus",
                CAST(o_totalprice AS DOUBLE) AS "orders.o_totalprice"
            FROM orders
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(ordersCSV),
	))

	// 3) Expected grouped result from DuckDB
	mustRunDuckDB(t, ctx, duckdbPath, dbPath, fmt.Sprintf(
		`COPY (
            SELECT
                CAST(customer.c_nationkey AS BIGINT) AS nationkey,
                CAST(SUM(orders.o_totalprice) AS DOUBLE) AS sum_totalprice
            FROM customer
            JOIN orders ON customer.c_custkey = orders.o_custkey
            WHERE orders.o_orderstatus = 'F'
            GROUP BY customer.c_nationkey
            ORDER BY nationkey
        ) TO '%s' (FORMAT CSV, HEADER, DELIMITER ',');`,
		escapeSingleQuotes(expectedCSV),
	))

	expected := mustReadExpectedNationKeyToSumTotalPrice(t, expectedCSV)
	customerBatch := mustReadCustomerBatchForJoin(t, customerCSV)
	ordersBatch := mustReadOrdersBatchForJoinWithStatusAndPrice(t, ordersCSV)

	// 4) Run DBSP on the same input
	query := `SELECT customer.c_nationkey, SUM(orders.o_totalprice)
		FROM customer JOIN orders ON customer.c_custkey = orders.o_custkey
		WHERE orders.o_orderstatus = 'F'
		GROUP BY customer.c_nationkey`
	root, err := ParseQueryToDBSP(query)
	if err != nil {
		t.Fatalf("ParseQueryToDBSP failed: %v", err)
	}

	g, ok := extractGroupAggOp(root.Op)
	if !ok {
		t.Fatalf("expected GroupAggOp in compiled plan, got %T", root.Op)
	}

	// Execute in two steps to avoid expensive ΔR⋈ΔS cross-product work.
	_, err = op.ExecuteTick(root, map[string]types.Batch{"customer": customerBatch})
	if err != nil {
		t.Fatalf("DBSP execute failed: %v", err)
	}
	_, err = op.ExecuteTick(root, map[string]types.Batch{"orders": ordersBatch})
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
			t.Fatalf("missing group key %v", k)
		}
		gotF, ok := toFloat64(v)
		if !ok {
			t.Fatalf("unexpected aggregate type for key %v: %T", k, v)
		}
		if !floatAlmostEqual(gotF, want, 1e-9, 1e-6) {
			t.Fatalf("sum mismatch for key %v: got %v, expected %v", k, gotF, want)
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

func tpchScaleFactor(t *testing.T) string {
	t.Helper()

	const defaultSF = 0.01

	raw := strings.TrimSpace(os.Getenv("DBSP_TPCH_SF"))
	if raw == "" {
		return strconv.FormatFloat(defaultSF, 'f', -1, 64)
	}

	sf, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("invalid DBSP_TPCH_SF %q: %v", raw, err)
	}
	if sf <= 0 {
		t.Fatalf("invalid DBSP_TPCH_SF %q: must be > 0", raw)
	}

	return strconv.FormatFloat(sf, 'f', -1, 64)
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

func mustReadOrdersStatusBatch(t *testing.T, csvPath string) types.Batch {
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
		tup := types.Tuple{"o_orderstatus": status}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadOrdersBatchWithPriority(t *testing.T, csvPath string) types.Batch {
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
	priorityIdx, ok := colIndex["o_orderpriority"]
	if !ok {
		t.Fatalf("orders csv missing column o_orderpriority; header=%v", header)
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
		priority := rec[priorityIdx]
		priceStr := rec[priceIdx]
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			t.Fatalf("parse price %q: %v", priceStr, err)
		}

		tup := types.Tuple{
			"o_orderstatus":   status,
			"o_orderpriority": priority,
			"o_totalprice":    price,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadLineitemBatch(t *testing.T, csvPath string) types.Batch {
	t.Helper()

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open lineitem csv: %v", err)
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

	flagIdx, ok := colIndex["l_returnflag"]
	if !ok {
		t.Fatalf("lineitem csv missing column l_returnflag; header=%v", header)
	}
	qtyIdx, ok := colIndex["l_quantity"]
	if !ok {
		t.Fatalf("lineitem csv missing column l_quantity; header=%v", header)
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

		flag := rec[flagIdx]
		qtyStr := rec[qtyIdx]
		qty, err := strconv.ParseFloat(qtyStr, 64)
		if err != nil {
			t.Fatalf("parse quantity %q: %v", qtyStr, err)
		}

		tup := types.Tuple{
			"l_returnflag": flag,
			"l_quantity":   qty,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadLineitemBatchWithLineStatus(t *testing.T, csvPath string) types.Batch {
	t.Helper()

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open lineitem csv: %v", err)
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

	flagIdx, ok := colIndex["l_returnflag"]
	if !ok {
		t.Fatalf("lineitem csv missing column l_returnflag; header=%v", header)
	}
	statusIdx, ok := colIndex["l_linestatus"]
	if !ok {
		t.Fatalf("lineitem csv missing column l_linestatus; header=%v", header)
	}
	qtyIdx, ok := colIndex["l_quantity"]
	if !ok {
		t.Fatalf("lineitem csv missing column l_quantity; header=%v", header)
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

		flag := rec[flagIdx]
		lineStatus := rec[statusIdx]
		qtyStr := rec[qtyIdx]
		qty, err := strconv.ParseFloat(qtyStr, 64)
		if err != nil {
			t.Fatalf("parse quantity %q: %v", qtyStr, err)
		}

		tup := types.Tuple{
			"l_returnflag": flag,
			"l_linestatus": lineStatus,
			"l_quantity":   qty,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadCustomerBatchForJoin(t *testing.T, csvPath string) types.Batch {
	t.Helper()

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open customer csv: %v", err)
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

	custKeyIdx, ok := colIndex["customer.c_custkey"]
	if !ok {
		t.Fatalf("customer csv missing column customer.c_custkey; header=%v", header)
	}
	nationKeyIdx, ok := colIndex["customer.c_nationkey"]
	if !ok {
		t.Fatalf("customer csv missing column customer.c_nationkey; header=%v", header)
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

		custKeyStr := rec[custKeyIdx]
		custKey, err := strconv.ParseInt(custKeyStr, 10, 64)
		if err != nil {
			t.Fatalf("parse customer key %q: %v", custKeyStr, err)
		}
		nationKeyStr := rec[nationKeyIdx]
		nationKey, err := strconv.ParseInt(nationKeyStr, 10, 64)
		if err != nil {
			t.Fatalf("parse nation key %q: %v", nationKeyStr, err)
		}

		tup := types.Tuple{
			"customer.c_custkey":   custKey,
			"customer.c_nationkey": nationKey,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadOrdersBatchForJoin(t *testing.T, csvPath string) types.Batch {
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

	custKeyIdx, ok := colIndex["orders.o_custkey"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_custkey; header=%v", header)
	}
	priceIdx, ok := colIndex["orders.o_totalprice"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_totalprice; header=%v", header)
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

		custKeyStr := rec[custKeyIdx]
		custKey, err := strconv.ParseInt(custKeyStr, 10, 64)
		if err != nil {
			t.Fatalf("parse orders customer key %q: %v", custKeyStr, err)
		}
		priceStr := rec[priceIdx]
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			t.Fatalf("parse price %q: %v", priceStr, err)
		}

		tup := types.Tuple{
			"orders.o_custkey":    custKey,
			"orders.o_totalprice": price,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadOrdersBatchForJoinWithStatus(t *testing.T, csvPath string) types.Batch {
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

	custKeyIdx, ok := colIndex["orders.o_custkey"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_custkey; header=%v", header)
	}
	statusIdx, ok := colIndex["orders.o_orderstatus"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_orderstatus; header=%v", header)
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

		custKeyStr := rec[custKeyIdx]
		custKey, err := strconv.ParseInt(custKeyStr, 10, 64)
		if err != nil {
			t.Fatalf("parse orders customer key %q: %v", custKeyStr, err)
		}
		status := rec[statusIdx]

		tup := types.Tuple{
			"orders.o_custkey":     custKey,
			"orders.o_orderstatus": status,
		}
		batch = append(batch, types.TupleDelta{Tuple: tup, Count: 1})
	}
	return batch
}

func mustReadOrdersBatchForJoinWithStatusAndPrice(t *testing.T, csvPath string) types.Batch {
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

	custKeyIdx, ok := colIndex["orders.o_custkey"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_custkey; header=%v", header)
	}
	statusIdx, ok := colIndex["orders.o_orderstatus"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_orderstatus; header=%v", header)
	}
	priceIdx, ok := colIndex["orders.o_totalprice"]
	if !ok {
		t.Fatalf("orders csv missing column orders.o_totalprice; header=%v", header)
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

		custKeyStr := rec[custKeyIdx]
		custKey, err := strconv.ParseInt(custKeyStr, 10, 64)
		if err != nil {
			t.Fatalf("parse orders customer key %q: %v", custKeyStr, err)
		}
		status := rec[statusIdx]
		priceStr := rec[priceIdx]
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			t.Fatalf("parse price %q: %v", priceStr, err)
		}

		tup := types.Tuple{
			"orders.o_custkey":     custKey,
			"orders.o_orderstatus": status,
			"orders.o_totalprice":  price,
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

func mustReadExpectedStatusToCount(t *testing.T, csvPath string) map[any]int64 {
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
	cntIdx, ok := colIndex["cnt"]
	if !ok {
		t.Fatalf("expected csv missing column cnt; header=%v", header)
	}

	out := make(map[any]int64)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		status := rec[statusIdx]
		cntStr := rec[cntIdx]
		cnt, err := strconv.ParseInt(cntStr, 10, 64)
		if err != nil {
			t.Fatalf("parse count %q: %v", cntStr, err)
		}
		out[status] = cnt
	}
	return out
}

func mustReadExpectedPriorityToSum(t *testing.T, csvPath string) map[any]float64 {
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

	priorityIdx, ok := colIndex["o_orderpriority"]
	if !ok {
		t.Fatalf("expected csv missing column o_orderpriority; header=%v", header)
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
		priority := rec[priorityIdx]
		sumStr := rec[sumIdx]
		sum, err := strconv.ParseFloat(sumStr, 64)
		if err != nil {
			t.Fatalf("parse sum %q: %v", sumStr, err)
		}
		out[priority] = sum
	}
	return out
}

func mustReadExpectedReturnFlagToSumQuantity(t *testing.T, csvPath string) map[any]float64 {
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

	flagIdx, ok := colIndex["l_returnflag"]
	if !ok {
		t.Fatalf("expected csv missing column l_returnflag; header=%v", header)
	}
	sumIdx, ok := colIndex["sum_quantity"]
	if !ok {
		t.Fatalf("expected csv missing column sum_quantity; header=%v", header)
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
		flag := rec[flagIdx]
		sumStr := rec[sumIdx]
		sum, err := strconv.ParseFloat(sumStr, 64)
		if err != nil {
			t.Fatalf("parse sum %q: %v", sumStr, err)
		}
		out[flag] = sum
	}
	return out
}

func mustReadExpectedLineStatusToSumQuantity(t *testing.T, csvPath string) map[any]float64 {
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

	statusIdx, ok := colIndex["l_linestatus"]
	if !ok {
		t.Fatalf("expected csv missing column l_linestatus; header=%v", header)
	}
	sumIdx, ok := colIndex["sum_quantity"]
	if !ok {
		t.Fatalf("expected csv missing column sum_quantity; header=%v", header)
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
		lineStatus := rec[statusIdx]
		sumStr := rec[sumIdx]
		sum, err := strconv.ParseFloat(sumStr, 64)
		if err != nil {
			t.Fatalf("parse sum %q: %v", sumStr, err)
		}
		out[lineStatus] = sum
	}
	return out
}

func mustReadExpectedNationKeyToSumTotalPrice(t *testing.T, csvPath string) map[any]float64 {
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

	nationIdx, ok := colIndex["nationkey"]
	if !ok {
		t.Fatalf("expected csv missing column nationkey; header=%v", header)
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

		nationStr := rec[nationIdx]
		nationKey, err := strconv.ParseInt(nationStr, 10, 64)
		if err != nil {
			t.Fatalf("parse nation key %q: %v", nationStr, err)
		}
		sumStr := rec[sumIdx]
		sum, err := strconv.ParseFloat(sumStr, 64)
		if err != nil {
			t.Fatalf("parse sum %q: %v", sumStr, err)
		}
		out[nationKey] = sum
	}
	return out
}

func mustReadExpectedJoinRowsMultiset(t *testing.T, csvPath string) map[string]int64 {
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

	nationIdx, ok := colIndex["nationkey"]
	if !ok {
		t.Fatalf("expected csv missing column nationkey; header=%v", header)
	}
	statusIdx, ok := colIndex["orderstatus"]
	if !ok {
		t.Fatalf("expected csv missing column orderstatus; header=%v", header)
	}

	out := make(map[string]int64)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		nationStr := rec[nationIdx]
		nationKey, err := strconv.ParseInt(nationStr, 10, 64)
		if err != nil {
			t.Fatalf("parse nation key %q: %v", nationStr, err)
		}
		status := rec[statusIdx]
		key := fmt.Sprintf("%d\t%s", nationKey, status)
		out[key]++
	}
	return out
}

func batchToJoinRowsMultiset(batch types.Batch) map[string]int64 {
	out := make(map[string]int64)
	for _, td := range batch {
		nationRaw, ok1 := td.Tuple["customer.c_nationkey"]
		statusRaw, ok2 := td.Tuple["orders.o_orderstatus"]
		if !ok1 || !ok2 {
			continue
		}
		nationKey, ok := toInt64(nationRaw)
		if !ok {
			continue
		}
		status, _ := statusRaw.(string)
		key := fmt.Sprintf("%d\t%s", nationKey, status)
		out[key] += td.Count
	}
	// Remove zeros if any
	for k, v := range out {
		if v == 0 {
			delete(out, k)
		}
	}
	return out
}

func multisetEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if bv, ok := b[k]; !ok || bv != av {
			return false
		}
	}
	return true
}

func mergeMultisets(a, b map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(a)+len(b))
	for k, v := range a {
		if v != 0 {
			out[k] = v
		}
	}
	for k, v := range b {
		out[k] += v
		if out[k] == 0 {
			delete(out, k)
		}
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

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		return int64(x), true
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
