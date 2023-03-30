#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/00-num-join-paths
mkdir ./experiment-results/00-num-join-paths

### DPhyp-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-equisets/job
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-equisets/tpch
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-equisets/ssb
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-equisets/ssb

### DPhyp-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-constant/job
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-constant/tpch
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/dphyp-constant/ssb
rm *.txt
mv *.csv experiment-results/00-num-join-paths/dphyp-constant/ssb

### Greedy-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-equisets/job
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-equisets/tpch
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-equisets/ssb
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-equisets/ssb

### Greedy-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-constant/job
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-constant/tpch
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --greedy_ordering --cardinalities=disabled --nruns=1
mkdir -p ./experiment-results/00-num-join-paths/greedy-constant/ssb
rm *.txt
mv *.csv experiment-results/00-num-join-paths/greedy-constant/ssb

python3 scripts/00-plot-num-join-paths.py