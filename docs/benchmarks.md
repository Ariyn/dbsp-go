# DBSP Baseline Benchmarks

This document records the baseline performance of the DBSP engine for standard scenarios using fixed input files.

## Environment

### Hardware
- Tool-provided environment

### Software
- OS: Linux
- Go Version: 1.25

## Scenarios

### 1. Single Table GroupAgg (Incremental)
- **Query**: `SELECT product, SUM(amount), COUNT(*) FROM bench_t GROUP BY product`
- **Data**: `benchmarks/data/bench_t.csv` (**10,000 rows**, 100 products)
- **Metric**: Execution time for the entire batch.

### 2. 2-way Join + GroupAgg (Incremental)
- **Query**: `SELECT a.category, SUM(b.amount) FROM a JOIN b ON a.id = b.a_id GROUP BY a.category`
- **Data**: `bench_a.csv` (**1,000 category mappings**), `bench_b.csv` (**10,000 transactions**)
- **Metric**: Execution time for both batches.

### 3. End-to-End CSV -> SQL -> Parquet
- **Source**: CSV
- **Transform**: `SELECT product, SUM(amount) FROM input GROUP BY product`
- **Data**: `bench_t.csv` (**10,000 rows**)
- **Sink**: Parquet
- **Metric**: Full pipeline execution time.

## Baseline Results (2026-02-22)

| Scenario | Iterations | Time/op | Bytes/op | Allocs/op |
| :------- | :--------- | :------ | :------- | :-------- |
| GroupAgg | 64 | 16.6 ms | 1.21 MB | 159,977 |
| JoinGroupAgg | 30 | 40.0 ms | 2.98 MB | 292,913 |
| E2E Parquet | 34 | 30.1 ms | 7.32 MB | 121,436 |

*Note: Initial numbers to be filled after running benchmarks.*
