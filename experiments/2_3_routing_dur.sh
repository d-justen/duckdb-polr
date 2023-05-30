#!/bin/bash

mkdir -p experiment-results

declare -a routing_strategies=(
  "init_once" "opportunistic" "adaptive_reinit" "dynamic"
)

declare -a optimizer_modes=(
  "dphyp-equisets" "dphyp-equisets-ldt"
)

declare -a benchmarks=(
  "imdb" "ssb"
)

# TODO: Add selection algo
DIR_NAME="2_3_routing_dur"
REGRET_BUDGET="0.01"

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/"${DIR_NAME}"
mkdir ./experiment-results/"${DIR_NAME}"

for optimizer_mode in "${optimizer_modes[@]}"
do
  for benchmark in "${benchmarks[@]}"
  do
      for strategy in "${routing_strategies[@]}"
      do
        ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --regret_budget="${REGRET_BUDGET}" --multiplexer_routing="${strategy}" --optimizer_mode="${optimizer_mode}" --threads=8 --nruns=20 --out=experiments/results.csv
        mkdir -p ./experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
        mv results.csv experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      done
  done
done

python3 scripts/plot_"${DIR_NAME}".py