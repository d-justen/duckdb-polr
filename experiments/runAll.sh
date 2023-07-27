#!/bin/bash

VENV_PATH="$PWD/venv"

dpkg -s clang-12 &> /dev/null
if [ $? -ne 0 ]; then
  sudo apt install clang-12
fi

dpkg -s libssl-dev &> /dev/null
if [ $? -ne 0 ]; then
  sudo apt install libssl-dev
fi

dpkg -s cgroup-tools &> /dev/null
if [ $? -ne 0 ]; then
  sudo apt install cgroup-tools
fi

lscgroup | grep limitcpu8 >/dev/null
if [ $? -ne 0 ]; then
  sudo cgcreate -g cpu:/limitcpu8
  sudo cgset -r cpu.cfs_quota_us=800000 limitcpu8
    sudo cgcreate -g cpu:/limitcpu1
    sudo cgset -r cpu.cfs_quota_us=100000 limitcpu1
fi

if [[ ! -d "$VENV_PATH" ]]; then
  echo "Creating Python Virtual Environment"
  python3 -m venv $VENV_PATH
  source "$VENV_PATH/bin/activate"
  pip install pip --upgrade > /dev/null
  pip -q install -r ../requirements.txt
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
sudo cgexec -g cpu:limitcpu1 ./2_3_routing_dur.sh

# echo "Running 3_1_perf_dur.sh..."
# ./3_1_perf_dur.sh

deactivate

#######
echo "Wrote results to ./experiment-results"