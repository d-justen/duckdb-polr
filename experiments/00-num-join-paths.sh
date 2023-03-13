#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.csv
rm -rf ./experiment-results/00-num-join-paths
mkdir ./experiment-results/00-num-join-paths

# Run Join Order Benchmark
mkdir ./experiment-results/00-num-join-paths/job
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mv *.csv experiment-results/00-num-join-paths/job

# Run TPC-H
mkdir ./experiment-results/00-num-join-paths/tpch
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mv *.csv experiment-results/00-num-join-paths/tpch

# Run Star-Schema Benchmark
mkdir ./experiment-results/00-num-join-paths/ssb
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mv *.csv experiment-results/00-num-join-paths/ssb

python3 scripts/00-plot-num-join-paths.py