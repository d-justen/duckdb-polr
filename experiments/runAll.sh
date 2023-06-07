#!/bin/bash

# Build
cd ..
BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_HTTPFS=1 make -j
cd experiments

# Cleanup
rm -rf experiment-results
chmod +x 1_1-2_sel_intms.sh
chmod +x 2_1-2_routing_intms.sh
chmod +x 2_3_routing_dur.sh
# chmod +x 3_1_perf_dur.sh

echo "Running 1_1-2_sel_intms.sh..."
sh 1_1-2_sel_intms.sh

echo "Running 2_1-2_routing_intms.sh..."
sh 2_1-2_routing_intms.sh

echo "Running 2_3_routing_dur.sh..."
sh 2_3_routing_dur.sh

# echo "Running 3_1_perf_dur.sh..."
# sh 3_1_perf_dur.sh

#######
echo "Wrote results to ./experiment-results"