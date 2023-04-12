#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/02-regret-budget
mkdir ./experiment-results/02-regret-budget

### DPhyp-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/optimum
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-00001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-0001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-01
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-1
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-2
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/0-8
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/1-6
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/2-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/job/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/job/3-2


## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/3-2

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1
mkdir -p ./experiment-results/02-regret-budget/dphyp-equisets/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-equisets/ssb/3-2

### DPhyp-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/optimum
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-00001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-0001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-01
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-1
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-2
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/0-8
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/1-6
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/2-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/job/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/job/3-2

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/3-2

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/dphyp-constant/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/dphyp-constant/ssb/3-2

### Greedy-equisets ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/optimum
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-00001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-0001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-01
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-1
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-2
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/0-8
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/1-6
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/2-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/job/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/job/3-2

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/3-2

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
mkdir -p ./experiment-results/02-regret-budget/greedy-equisets/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-equisets/ssb/3-2

### Greedy-constant ###
## Run Join Order Benchmark
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/optimum
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-00001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-0001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-001
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-01
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-1
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-2
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/0-8
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/1-6
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/2-4
../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/job/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/job/3-2

## Run TPC-H
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/3-2

## Run Star-Schema Benchmark
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --multiplexer_routing=alternate --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/optimum
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/optimum
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.00001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-00001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-00001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.0001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-0001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-0001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.001 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-001
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-001
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.01 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-01
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-01
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.1 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-1
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-1
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-2
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=0.8 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/0-8
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/0-8
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=1.6 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/1-6
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/1-6
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=2.4 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/2-4
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/2-4
../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget=3.2 --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
mkdir -p ./experiment-results/02-regret-budget/greedy-constant/ssb/3-2
mv *.{csv,txt} experiment-results/02-regret-budget/greedy-constant/ssb/3-2

python3 scripts/02-plot-regret-budget.py