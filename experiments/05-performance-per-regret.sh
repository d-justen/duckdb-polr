#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/05-performance-per-regret
mkdir ./experiment-results/05-performance-per-regret

declare -a optimizer_modes=(
  "dphyp-equisets" "dphyp-constant"
)

declare -a static_routing_strategies=(
  "init_once" "opportunistic" "default_path"
)

declare -a adaptive_routing_strategies=(
  "adaptive_reinit" "dynamic"
)

declare -a regret_budgets=(
  "0.00001" "0.0001" "0.001" "0.01" "0.1" "0.2" "0.4" "0.8" "1.6" "2.4" "3.2"
)

for optimizer_mode in "${optimizer_modes[@]}"
do
  # JOB
   ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --threads=8 --nruns=20 --out=experiments/std.csv
  mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/std
  mv std.csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/std

  for strategy in "${static_routing_strategies[@]}"
  do
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=20 --out=experiments/"${strategy}".csv --optimizer_mode="${optimizer_mode}" --multiplexer_routing="${strategy}"
    mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/"${strategy}"
    mv "${strategy}".csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/"${strategy}"
  done

  for strategy in "${adaptive_routing_strategies[@]}"
  do
      for regret_budget in "${regret_budgets[@]}"
      do
        ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=8 --nruns=20 --out=experiments/"${strategy}"-"${regret_budget}".csv --optimizer_mode="${optimizer_mode}" --multiplexer_routing="${strategy}" --regret_budget="${regret_budget}"
        mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/"${strategy}"
        mv "${strategy}"-"${regret_budget}".csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/job/"${strategy}"
    done
  done

  # SSB
   ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --threads=8 --nruns=20 --out=experiments/std.csv
  mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/std
  mv std.csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/std

  for strategy in "${static_routing_strategies[@]}"
  do
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=20 --out=experiments/"${strategy}".csv --optimizer_mode="${optimizer_mode}" --multiplexer_routing="${strategy}"
    mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/"${strategy}"
    mv "${strategy}".csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/"${strategy}"
  done

  for strategy in "${adaptive_routing_strategies[@]}"
  do
      for regret_budget in "${regret_budgets[@]}"
      do
        ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=8 --nruns=20 --out=experiments/"${strategy}"-"${regret_budget}".csv --optimizer_mode="${optimizer_mode}" --multiplexer_routing="${strategy}" --regret_budget="${regret_budget}"
        mkdir -p ./experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/"${strategy}"
        mv "${strategy}"-"${regret_budget}".csv experiment-results/05-performance-per-regret/"${optimizer_mode}"/ssb/"${strategy}"
    done
  done
done

python3 scripts/05-plot-performance-per-regret.py