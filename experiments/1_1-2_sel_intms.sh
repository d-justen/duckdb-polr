#!/bin/bash

mkdir -p experiment-results

declare -a enumeration_strategies=(
  "each_last_once" "each_first_once" "dfs_random" "dfs_min_card" "dfs_uncertain" "bfs_random" "bfs_min_card" "bfs_uncertain"
)

declare -a optimizer_modes=(
  "dphyp-equisets" "greedy-equisets-ldt" "nostats"
)

declare -a benchmarks=(
  "imdb" "ssb"
)

DIR_NAME_1="1_1_sel_intms"
DIR_NAME_2="1_2_sel_dur"

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/"${DIR_NAME_1}"
mkdir ./experiment-results/"${DIR_NAME_1}"
rm -rf ./experiment-results/"${DIR_NAME_2}"
mkdir ./experiment-results/"${DIR_NAME_2}"

for optimizer_mode in "${optimizer_modes[@]}"
do
  for benchmark in "${benchmarks[@]}"
  do
    for strategy in "${enumeration_strategies[@]}"
    do
      ../build/release/benchmark/benchmark_runner "benchmark/${benchmark}/.*" --polr_mode=bushy --multiplexer_routing=alternate --enumerator="${strategy}" --threads=1 --log_tuples_routed --disable_caching --nruns=1 --optimizer_mode="${optimizer_mode}"
      mkdir -p ./experiment-results/"${DIR_NAME_2}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      mv *-enumeration.csv experiment-results/"${DIR_NAME_2}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      mkdir -p ./experiment-results/"${DIR_NAME_1}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
      mv *.{csv,txt} experiment-results/"${DIR_NAME_1}"/"${optimizer_mode}"/${benchmark}/"${strategy}"
    done
  done
done

python3 scripts/plot_"${DIR_NAME_1}".py
python3 scripts/plot_"${DIR_NAME_2}".py