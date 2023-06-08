#!/bin/bash

mkdir -p experiment-results

declare -a benchmarks=(
  "imdb" "ssb"
)

# TODO: Add selection algo
DIR_NAME="3_1_perf_dur"
ROUTING_STRATEGY="dynamic"
REGRET_BUDGET="0.01"

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/"${DIR_NAME}"
mkdir ./experiment-results/"${DIR_NAME}"

for benchmark in "${benchmarks[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --regret_budget="${REGRET_BUDGET}" --multiplexer_routing="${strategy}" --optimizer_mode="${optimizer_mode}" --threads=8 --nruns=20 --out=experiments/results.csv
  mkdir -p ./experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/
  mv results.csv experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/
done

#TODO: Postgres
#TODO: Skinner
#TODO: MonetDB

python3 scripts/plot_"${DIR_NAME}".py