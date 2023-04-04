#!/bin/bash

# Build
cd ..
BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_HTTPFS=1 make -j
cd experiments

# Cleanup
rm -rf experiment-results

# Run 00-num-join-paths
echo "Running 00-num-join-paths..."
sh 00-num-join-paths.sh

# Run 01-num-intermediates
echo "Running 01-num-intermediates..."
sh 01-num-intermediates.sh

# Run 02-regret-budget
echo "Running 02-regret-budget..."
sh 02-regret-budget.sh

# Run 02-routing-efficacy
echo "03-pipeline-performance..."
sh 03-pipeline-performance.sh

# Run 03-performance
echo "Running 04-performance..."
sh 04-performance.sh

#######
echo "Wrote results to ./experiment-results"