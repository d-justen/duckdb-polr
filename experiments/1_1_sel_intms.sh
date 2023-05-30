#!/bin/bash

mkdir -p experiment-results

declare -a enumeration_strategies=(
  "exhaustive" "each_last_once"
)

declare -a optimizer_modes=(
  "dphyp-equisets" "greedy-equisets-ldt"
)

declare -a benchmarks=(
  "imdb" "ssb"
)

DIR_NAME="1_1_sel_intms"

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/"${DIR_NAME}"
mkdir ./experiment-results/"${DIR_NAME}"

for optimizer_mode in "${optimizer_modes[@]}"
do
  for benchmark in "${benchmarks[@]}"
  do
    for strategy in "${enumeration_strategies[@]}"
    do
      ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --multiplexer_routing=alternate --enumerator="${strategy}" --threads=1 --log_tuples_routed --disable_caching --nruns=1 --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      mv *.{csv,txt} experiment-results/"${DIR_NAME}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
    done
  done
done

python3 scripts/plot_"${DIR_NAME}".py