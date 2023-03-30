#!/bin/bash

mkdir -p experiment-results

# Cleanup
rm -f *.{csv,txt}
rm -rf ./experiment-results/03-performance
mkdir ./experiment-results/03-performance

declare -a job_queries=(
  "01a" "01b" "01c" "01d" "02a" "02b" "02c" "02d" "03a" "03b" "03c"
  "04a" "04b" "04c" "05a" "05b" "05c" "06a" "06b" "06c" "06d" "06e"
  "06f" "07a" "07b" "07c" "08a" "08b" "08c" "08d" "09a" "09b" "09c"
  "09d" "10a" "10b" "10c" "11a" "11b" "11c" "11d" "12a" "12b" "12c"
  "13a" "13b" "13c" "13d" "14a" "14b" "14c" "15a" "15b" "15c" "15d"
  "16a" "16b" "16c" "16d" "17a" "17b" "17c" "17d" "17e" "17f" "18a"
  "18b" "18c" "19a" "19b" "19c" "19d" "20a" "20b" "20c" "21a" "21b"
  "21c" "22a" "22b" "22c" "22d" "23a" "23b" "23c" "24a" "24b" "25a"
  "25b" "25c" "26a" "26b" "26c" "27a" "27b" "27c" "28a" "28b" "28c"
  "29a" "29b" "29c" "30a" "30b" "30c" "31a" "31b" "31c" "32a" "32b"
  "33a" "33b" "33c"
)

declare -a ssb_queries=(
  "q1-1" "q1-2" "q1-3" "q2-1" "q2-2" "q2-3" "q3-1" "q3-2" "q3-3" "q4-1"
  "q4-2" "q4-3"
)

### DPhyp-equisets ###
## Run Join Order Benchmark
mkdir -p ./experiment-results/03-performance/dphyp-equisets/job/polr
mkdir -p ./experiment-results/03-performance/dphyp-equisets/job/std
for query in "${job_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-equisets/job/polr
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --threads=8 --nruns=20 --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-equisets/job/std
  fi
done

## Run Star-Schema Benchmark
mkdir -p ./experiment-results/03-performance/dphyp-equisets/ssb/polr
mkdir -p ./experiment-results/03-performance/dphyp-equisets/ssb/std
for query in "${ssb_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-equisets/ssb/polr
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --threads=8 --nruns=20 --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-equisets/ssb/std
  fi
done

### DPhyp-constant ###
## Run Join Order Benchmark
mkdir -p ./experiment-results/03-performance/dphyp-constant/job/polr
mkdir -p ./experiment-results/03-performance/dphyp-constant/job/std
for query in "${job_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-constant/job/polr
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --threads=8 --nruns=20 --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-constant/job/std
  fi
done

## Run Star-Schema Benchmark
mkdir -p ./experiment-results/03-performance/dphyp-constant/ssb/polr
mkdir -p ./experiment-results/03-performance/dphyp-constant/ssb/std
for query in "${ssb_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --cardinalities=disabled
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-constant/ssb/polr
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --threads=8 --nruns=20 --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/dphyp-constant/ssb/std
  fi
done

### Greedy-equisets ###
## Run Join Order Benchmark
mkdir -p ./experiment-results/03-performance/greedy-equisets/job/polr
mkdir -p ./experiment-results/03-performance/greedy-equisets/job/std
for query in "${job_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --greedy_ordering --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-equisets/job/polr
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --threads=8 --nruns=20 --greedy_ordering --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-equisets/job/std
  fi
done

## Run Star-Schema Benchmark
mkdir -p ./experiment-results/03-performance/greedy-equisets/ssb/polr
mkdir -p ./experiment-results/03-performance/greedy-equisets/ssb/std
for query in "${ssb_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --greedy_ordering --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-equisets/ssb/polr
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --threads=8 --nruns=20 --greedy_ordering --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-equisets/ssb/std
  fi
done

### Greedy-constant ###
## Run Join Order Benchmark
mkdir -p ./experiment-results/03-performance/greedy-constant/job/polr
mkdir -p ./experiment-results/03-performance/greedy-constant/job/std
for query in "${job_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --greedy_ordering --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-constant/job/polr
    ../build/release/benchmark/benchmark_runner "benchmark/imdb/${query}.benchmark" --threads=8 --nruns=20 --greedy_ordering --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-constant/job/std
  fi
done

## Run Star-Schema Benchmark
mkdir -p ./experiment-results/03-performance/greedy-constant/ssb/polr
mkdir -p ./experiment-results/03-performance/greedy-constant/ssb/std
for query in "${ssb_queries[@]}"
do
  ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=1 --log_tuples_routed --nruns=1 --greedy_ordering --cardinalities=disabled
  if compgen -G *.csv > /dev/null; then
    rm -f *.{csv,txt}
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --polr_mode=bushy --threads=8 --nruns=20 --regret_budget=0.01 --greedy_ordering --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-constant/ssb/polr
    ../build/release/benchmark/benchmark_runner "benchmark/ssb/${query}.benchmark" --threads=8 --nruns=20 --greedy_ordering --cardinalities=disabled --out=experiments/"${query}".csv
    mv "${query}".csv experiment-results/03-performance/greedy-constant/ssb/std
  fi
done

python3 scripts/03-plot-performance.py