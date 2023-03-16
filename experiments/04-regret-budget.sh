#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/04-regret-budget
mkdir ./experiment-results/04-regret-budget

# Run Join Order Benchmark
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
mkdir ./experiment-results/04-regret-budget/job
mv *.csv experiment-results/04-regret-budget/job

# Run Star-Schema Benchmark
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
mkdir ./experiment-results/04-regret-budget/ssb
mv *.csv experiment-results/04-regret-budget/ssb

python3 scripts/04-regret-budget.py