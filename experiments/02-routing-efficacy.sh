#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/02-routing-efficacy
mkdir ./experiment-results/02-routing-efficacy

# Get query names
#../build/release/benchmark/benchmark_runner --list | grep imdb | sed 's/benchmark\/imdb\///g' | sed s/.benchmark//g | sort -n > tmp_job_names

# Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/02-routing-efficacy/job
mv *.{csv,txt} experiment-results/02-routing-efficacy/job

# Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/02-routing-efficacy/tpch
mv *.{csv,txt} experiment-results/02-routing-efficacy/tpch

# Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir ./experiment-results/02-routing-efficacy/ssb
mv *.{csv,txt} experiment-results/02-routing-efficacy/ssb

python3 scripts/02-plot-routing-efficacy.py