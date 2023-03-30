#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/02-routing-efficacy
mkdir ./experiment-results/02-routing-efficacy

# Get query names
#../build/release/benchmark/benchmark_runner --list | grep imdb | sed 's/benchmark\/imdb\///g' | sed s/.benchmark//g | sort -n > tmp_job_names

### DPhyp-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-equisets/job
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-equisets/tpch
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-equisets/ssb
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-equisets/ssb

### DPhyp-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-constant/job
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-constant/tpch
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/dphyp-constant/ssb
mv *.{csv,txt} experiment-results/02-routing-efficacy/dphyp-constant/ssb

### Greedy-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/greedy-equisets/job
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/greedy-equisets/tpch
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/02-routing-efficacy/greedy-equisets/ssb
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-equisets/ssb

### Greedy-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/greedy-constant/job
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/greedy-constant/tpch
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-routing-efficacy/greedy-constant/ssb
mv *.{csv,txt} experiment-results/02-routing-efficacy/greedy-constant/ssb

python3 scripts/02-plot-routing-efficacy.py