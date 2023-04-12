#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/01-num-intermediates
mkdir ./experiment-results/01-num-intermediates

### DPhyp-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/dphyp-equisets/job
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/dphyp-equisets/tpch
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/dphyp-equisets/ssb
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-equisets/ssb

### DPhyp-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/dphyp-constant/job
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/dphyp-constant/tpch
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/dphyp-constant/ssb
rm *.txt
mv *.csv experiment-results/01-num-intermediates/dphyp-constant/ssb

### Greedy-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/greedy-equisets/job
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/greedy-equisets/tpch
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-equisets/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate
mkdir -p ./experiment-results/01-num-intermediates/greedy-equisets/ssb
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-equisets/ssb

### Greedy-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/greedy-constant/job
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/greedy-constant/tpch
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-constant/tpch

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --multiplexer_routing=alternate --cardinalities=disabled
mkdir -p ./experiment-results/01-num-intermediates/greedy-constant/ssb
rm *.txt
mv *.csv experiment-results/01-num-intermediates/greedy-constant/ssb

python3 scripts/01-plot-num-intermediates.py