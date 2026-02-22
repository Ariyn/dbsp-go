#!/bin/bash
mkdir -p benchmarks/data
echo "product,amount" > benchmarks/data/bench_t.csv
for i in {1..10000}; do
  echo "p$((i%100 + 1)),$((i*10))" >> benchmarks/data/bench_t.csv
done

echo "id,category" > benchmarks/data/bench_a.csv
for i in {1..1000}; do
  echo "$i,cat$((i%50 + 1))" >> benchmarks/data/bench_a.csv
done

echo "a_id,amount" > benchmarks/data/bench_b.csv
for i in {1..10000}; do
  echo "$((i%1000 + 1)),$((i*5))" >> benchmarks/data/bench_b.csv
done
