#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/03-performance
mkdir ./experiment-results/03-performance

# Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --threads=8 --nruns=20 --out=experiments/job.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.1 --out=experiments/job-polr.csv
mkdir ./experiment-results/03-performance/job
mv job.csv job-polr.csv experiment-results/03-performance/job

# Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --threads=8 --nruns=20 --out=experiments/tpch.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.1 --out=experiments/tpch-polr.csv
mkdir ./experiment-results/03-performance/tpch
mv tpch.csv tpch-polr.csv experiment-results/03-performance/tpch

# Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --threads=8 --nruns=20 --out=experiments/ssb.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.1 --out=experiments/ssb-polr.csv
mkdir ./experiment-results/03-performance/ssb
mv ssb.csv ssb-polr.csv experiment-results/03-performance/ssb

python3 scripts/03-plot-performance.py