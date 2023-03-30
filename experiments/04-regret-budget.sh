#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/04-regret-budget
mkdir ./experiment-results/04-regret-budget

### DPhyp-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv
mkdir -p ./experiment-results/04-regret-budget/dphyp-equisets/job
mv *.csv experiment-results/04-regret-budget/dphyp-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv
mkdir -p ./experiment-results/04-regret-budget/dphyp-equisets/ssb
mv *.csv experiment-results/04-regret-budget/dphyp-equisets/ssb

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv
mkdir -p ./experiment-results/04-regret-budget/dphyp-equisets/ssb
mv *.csv experiment-results/04-regret-budget/dphyp-equisets/ssb

### DPhyp-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/dphyp-constant/job
mv *.csv experiment-results/04-regret-budget/dphyp-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/dphyp-constant/ssb
mv *.csv experiment-results/04-regret-budget/dphyp-constant/ssb

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/dphyp-constant/ssb
mv *.csv experiment-results/04-regret-budget/dphyp-constant/ssb

### Greedy-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering
mkdir -p ./experiment-results/04-regret-budget/greedy-equisets/job
mv *.csv experiment-results/04-regret-budget/greedy-equisets/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering
mkdir -p ./experiment-results/04-regret-budget/greedy-equisets/ssb
mv *.csv experiment-results/04-regret-budget/greedy-equisets/ssb

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering
mkdir -p ./experiment-results/04-regret-budget/greedy-equisets/ssb
mv *.csv experiment-results/04-regret-budget/greedy-equisets/ssb

### Greedy-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/greedy-constant/job
mv *.csv experiment-results/04-regret-budget/greedy-constant/job

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/greedy-constant/ssb
mv *.csv experiment-results/04-regret-budget/greedy-constant/ssb

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.0001 --out=experiments/0_0001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.001 --out=experiments/0_001.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.01 --out=experiments/0_01.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.1 --out=experiments/0_1.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.2 --out=experiments/0_2.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.4 --out=experiments/0_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=0.8 --out=experiments/0_8.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=1.6 --out=experiments/1_6.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=2.4 --out=experiments/2_4.csv --greedy_ordering --cardinalities=disabled
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=5 --regret_budget=3.2 --out=experiments/3_2.csv --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/04-regret-budget/greedy-constant/ssb
mv *.csv experiment-results/04-regret-budget/greedy-constant/ssb

python3 scripts/04-regret-budget.py