#!/bin/bash

mkdir -p experiment-results

declare -a static_routing_strategies=(
  "alternate" "default_path" "init_once" "opportunistic"
)

# TODO add backpressure(?)
declare -a adaptive_routing_strategies=(
  "adaptive_reinit" "dynamic"
)

declare -a regret_budgets=(
  "0.01"
)

declare -a optimizer_modes=(
  "dphyp-equisets"
)

declare -a benchmarks=(
 "ssb-skew"
)

# TODO: Add selection algo
DIR_NAME="2_1-2_routing_intms"

# Cleanup
rm -rf ../tmp
mkdir -p ../tmp
rm -rf ./experiment-results/"${DIR_NAME}"
mkdir ./experiment-results/"${DIR_NAME}"

for optimizer_mode in "${optimizer_modes[@]}"
do
  for benchmark in "${benchmarks[@]}"
  do
    for strategy in "${static_routing_strategies[@]}"
    do
      ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --disable_caching --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      rm ../tmp/*-enumeration.csv
      mv ../tmp/*.{csv,txt} experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
    done

    for strategy in "${adaptive_routing_strategies[@]}"
    do
      for regret_budget in "${regret_budgets[@]}"
      do
        ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --regret_budget="${regret_budget}" --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --disable_caching --optimizer_mode="${optimizer_mode}"
        mkdir -p ./experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"/"${regret_budget}"
        rm ../tmp/*-enumeration.csv
        mv ../tmp/*.{csv,txt} experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"/"${regret_budget}"
      done
    done
  done
done

# python3 scripts/plot_2_1_routing_expl_intms.py
python3 scripts/plot_2_2_routing_all_intms.py
