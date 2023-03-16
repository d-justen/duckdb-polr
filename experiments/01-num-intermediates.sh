#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/01-num-intermediates
mkdir ./experiment-results/01-num-intermediates

# Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --mpx_alternate_chunks
mkdir ./experiment-results/01-num-intermediates/job
rm *.txt
mv *.csv experiment-results/01-num-intermediates/job

# Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --mpx_alternate_chunks
mkdir ./experiment-results/01-num-intermediates/tpch
rm *.txt
mv *.csv experiment-results/01-num-intermediates/tpch

# Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --mpx_alternate_chunks
mkdir ./experiment-results/01-num-intermediates/ssb
rm *.txt
mv *.csv experiment-results/01-num-intermediates/ssb

python3 scripts/01-plot-num-intermediates.py