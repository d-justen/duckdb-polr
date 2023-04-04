#!/usr/bin/env python3

import pandas as pd
import os
import glob
from statistics import mean
import numpy as np

query_counts = {"job": 113, "ssb": 12}
modes = ["dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"]
results = {}

for mode in modes:
    results[mode] = {}
    for benchmark_name in query_counts:
        path = os.getcwd() + "/experiment-results/03-pipeline-performance/" + mode + "/" + benchmark_name

        polr_durations = glob.glob(os.path.join(path + "/polr", "*.csv"))
        polr_durations.sort()

        polr_results = []
        benchmark_ids = []

        for file in polr_durations:
            benchmark_id = file.split("/")[-1].split(".")[0]
            benchmark_ids.append(benchmark_id)
            df = pd.read_csv(file, names=["timing"])
            polr_results.append(float(df.mean()["timing"]))

        std_files = glob.glob(os.path.join(path + "/std", "*.csv"))
        std_files.sort()

        std_results = []

        for file in std_files:
            benchmark_id = file.split("/")[-1].split(".")[0]
            df = pd.read_csv(file, names=["timing"])
            std_results.append(float(df.mean()["timing"]))

        results[mode][benchmark_name] = {"polr": polr_results, "std": std_results}

        for i in range(len(benchmark_ids)):
            rel = polr_results[i] / std_results[i] - 1
            print(benchmark_ids[i] + ": " + str(rel))