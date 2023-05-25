#!/bin/bash

mkdir -p experiment-results

declare -a optimizer_modes=(
  "dphyp-equisets" "dphyp-constant" "greedy-equisets" "greedy-constant"
)

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/01-num-intermediates
mkdir ./experiment-results/01-num-intermediates

for optimizer_mode in "${optimizer_modes[@]}"
do
  ## Run Join Order Benchmark
  ../build/release/benchmark/benchmark_runner "benchmark/imdb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --disable_caching --optimizer_mode="${optimizer_mode}"
  mkdir -p ./experiment-results/01-num-intermediates/"${optimizer_mode}"/job
  rm *.txt
  mv *.csv experiment-results/01-num-intermediates/"${optimizer_mode}"/job

  ## Run TPC-H
  ../build/release/benchmark/benchmark_runner "benchmark/tpch/sf1/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --disable_caching --optimizer_mode="${optimizer_mode}"
  mkdir -p ./experiment-results/01-num-intermediates/"${optimizer_mode}"/tpch
  rm *.txt
  mv *.csv experiment-results/01-num-intermediates/"${optimizer_mode}"/tpch

  ## Run Star-Schema Benchmark
  ../build/release/benchmark/benchmark_runner "benchmark/ssb/.*" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --multiplexer_routing=alternate --disable_caching --optimizer_mode="${optimizer_mode}"
  mkdir -p ./experiment-results/01-num-intermediates/"${optimizer_mode}"/ssb
  rm *.txt
  mv *.csv experiment-results/01-num-intermediates/"${optimizer_mode}"/ssb
done

python3 scripts/01-plot-num-intermediates.py