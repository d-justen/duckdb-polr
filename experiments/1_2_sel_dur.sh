#!/bin/bash

mkdir -p experiment-results

declare -a enumeration_strategies=(
  "exhaustive" "each-last-once"
)

declare -a optimizer_modes=(
  "dphyp-equisets" "greedy-equisets-ldt"
)

declare -a benchmarks=(
  "imdb" "ssb"
)

# TODO: Add selection algo
DIR_NAME="1_2_sel_dur"

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/"${DIR_NAME}"
mkdir ./experiment-results/"${DIR_NAME}"

# TODO: Map number of joins in pipeline and stop time for selection algo

python3 scripts/plot_"${DIR_NAME}".py