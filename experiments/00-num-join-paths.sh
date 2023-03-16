#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/00-num-join-paths
mkdir ./experiment-results/00-num-join-paths

# Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/00-num-join-paths/job
rm *.txt
mv *.csv experiment-results/00-num-join-paths/job

# Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/00-num-join-paths/tpch
rm *.txt
mv *.csv experiment-results/00-num-join-paths/tpch

# Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/00-num-join-paths/ssb
rm *.txt
mv *.csv experiment-results/00-num-join-paths/ssb

python3 scripts/00-plot-num-join-paths.py