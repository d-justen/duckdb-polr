#!/bin/bash

VENV_PATH="$PWD/venv"

if [[ ! dpkg -s clang-12 &> /dev/null ]]; then
  sudo apt install clang-12
fi

if [[ ! dpkg -s libssl-dev &> /dev/null ]]; then
  sudo apt install libssl-dev
fi

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python Virtual Environment"
  python3 -m venv $VENV_PATH
  source "$VENV_PATH/bin/activate"
  pip install pip --upgrade > /dev/null
  pip -q install -r requirements.txt
  echo "$HOSTNAME"
fi

# Build
cd ..
BUILD_BENCHMARK=1 BUILD_TPCH=1 BUILD_HTTPFS=1 make CC=/usr/bin/clang-12 CXX=/usr/bin/clang++-12 -j
cd experiments

# Cleanup
rm -rf experiment-results
chmod +x 1_1-2_sel_intms.sh
chmod +x 2_1-2_routing_intms.sh
chmod +x 2_3_routing_dur.sh
# chmod +x 3_1_perf_dur.sh

echo "Running 1_1-2_sel_intms.sh..."
./1_1-2_sel_intms.sh

echo "Running 2_1-2_routing_intms.sh..."
./2_1-2_routing_intms.sh

echo "Running 2_3_routing_dur.sh..."
./2_3_routing_dur.sh

# echo "Running 3_1_perf_dur.sh..."
# ./3_1_perf_dur.sh

deactivate

#######
echo "Wrote results to ./experiment-results"