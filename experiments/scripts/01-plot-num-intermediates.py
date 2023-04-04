#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
from statistics import mean

query_counts = {"job": 113, "tpch": 22, "ssb": 12}
results = {}
modes = ["dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"]

plt_idx = 1
bins = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]

for i in range(len(modes)):
    mode = modes[i]
    results[mode] = {}

    for j in range(len(query_counts)):
        benchmark_name = list(query_counts.keys())[j]
        path = os.getcwd() + "/experiment-results/01-num-intermediates/" + mode + "/" + benchmark_name
        print("Processing " + benchmark_name)
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        csv_files.sort()

        duckdb_savings = []
        glob_opt_savings = []
        intms_duckdb = []
        intms_polr = []
        intms_glob_opt = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)

            # Cleaning
            df.pop(df.columns[-1])

            duckdb_intm_sum = df["path_0"].sum()
            polr_intm_sum = df.min(axis=1).sum()
            glob_opt_sum = df.sum().min()

            intms_duckdb.append(duckdb_intm_sum)
            intms_polr.append(polr_intm_sum)
            intms_glob_opt.append(glob_opt_sum)

            if duckdb_intm_sum > 0:
                duckdb_savings.append(1 - (polr_intm_sum / duckdb_intm_sum))
            else:
                duckdb_savings.append(0)

            if glob_opt_sum > 0:
                glob_opt_savings.append(1 - (polr_intm_sum / glob_opt_sum))
            else:
                glob_opt_savings.append(0)

        duckdb_saving = 1 - sum(intms_polr) / sum(intms_duckdb)
        glob_opt_saving = 1 - sum(intms_polr) / sum(intms_glob_opt)
        results[mode][benchmark_name] = {"duckdb_saving": duckdb_saving, "glob_opt_saving": glob_opt_saving}

        plt.subplot(len(modes), len(query_counts), plt_idx)
        plt.hist(glob_opt_savings, bins=bins)
        if i == 0:
            plt.title(benchmark_name)
        if j == 0:
            short_mode_split = mode.split("-")
            short_mode = short_mode_split[0][:2] + short_mode_split[1][:2]
            plt.ylabel(short_mode)
        elif j == len(query_counts) - 1:
            plt.ylabel("# queries")
        if i == len(modes) - 1:
            plt.xlabel("Savings")
        plt_idx += 1

plt.tight_layout()
plt.savefig("experiment-results/01-num-intermediates.pdf")

experiment_output = "config,"

for benchmark_name in query_counts:
    experiment_output += benchmark_name + ","
experiment_output = experiment_output[:-1] + "\n"

experiment_output2 = experiment_output

for mode in results:
    experiment_output += mode + ","
    experiment_output2 += mode + ","
    for benchmark_name in query_counts:
        experiment_output += "{0:.0%}".format(results[mode][benchmark_name]["duckdb_saving"]) + ","
        experiment_output2 += "{0:.0%}".format(results[mode][benchmark_name]["glob_opt_saving"]) + ","
    experiment_output = experiment_output[:-1] + "\n"
    experiment_output2 = experiment_output2[:-1] + "\n"

with open("experiment-results/01-num-intermediates-optimizer-choice.csv", "w") as file:
    file.write(experiment_output)

with open("experiment-results/01-num-intermediates-glob-optimal-choice.csv", "w") as file:
    file.write(experiment_output2)
