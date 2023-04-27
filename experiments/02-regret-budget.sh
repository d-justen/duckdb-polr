#!/bin/bash

mkdir -p experiment-results

declare -a optimizer_modes=(
  "dphyp-equisets" "dphyp-constant" "greedy-equisets" "greedy-constant"
)

declare -a static_routing_strategies=(
  "alternate" "init_once" "opportunistic"
)

declare -a adaptive_routing_strategies=(
  "adaptive_reinit" "dynamic"
)

declare -a regret_budgets=(
  "0.00001" "0.0001" "0.001" "0.01" "0.1" "0.2" "0.4" "0.8" "1.6" "2.4" "3.2"
)

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/02-regret-budget
mkdir ./experiment-results/02-regret-budget

for optimizer_mode in "${optimizer_modes[@]}"
do
  for strategy in "${static_routing_strategies[@]}"
  do
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
    mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/job
    mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/job

    ../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
    mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/tpch
    mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/tpch

    ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
    mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/ssb
    mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/ssb
  done

  for strategy in "${adaptive_routing_strategies[@]}"
  do
    for regret_budget in "${regret_budgets[@]}"
    do
      ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --regret_budget="${regret_budget}" --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/job/"${regret_budget}"
      mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/job/"${regret_budget}"

      ../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --regret_budget="${regret_budget}" --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/tpch/"${regret_budget}"
      mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/tpch/"${regret_budget}"

      ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --regret_budget="${regret_budget}" --multiplexer_routing="${strategy}" --threads=1 --log_tuples_routed --nruns=1 --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/ssb/"${regret_budget}"
      mv *.{csv,txt} experiment-results/02-regret-budget/"${optimizer_mode}"/"${strategy}"/ssb/"${regret_budget}"
    done
  done
done

python3 scripts/02-plot-regret-budget.py