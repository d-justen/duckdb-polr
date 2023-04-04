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
        path = os.getcwd() + "/experiment-results/03-performance/" + mode + "/" + benchmark_name

        polr_durations = glob.glob(os.path.join(path + "/polr", "*.csv"))
        polr_durations.sort()

        polr_results = []

        for file in polr_durations:
            benchmark_id = file.split("/")[-1].split(".")[0]
            df = pd.read_csv(file, names=["name", "run", "timing"])
            polr_results.append(float(df.groupby("name").mean()["timing"]))

        std_files = glob.glob(os.path.join(path + "/std", "*.csv"))
        std_files.sort()

        std_results = []

        for file in std_files:
            benchmark_id = file.split("/")[-1].split(".")[0]
            df = pd.read_csv(file, names=["name", "run", "timing"])
            std_results.append(float(df.groupby("name").mean()["timing"]))

        results[mode][benchmark_name] = {"polr": polr_results, "std": std_results}

result_str = ""

for benchmark_name in query_counts:
    result_str += benchmark_name + "\n"
    result_str += "\\begin{tabular}{lrrr}\n"
    for mode in modes:
        result_str += "\\multicolumn{3}{l}{\\textbf{" + mode + "}}\\\\\n\\hline\n"
        result_str += " & POLAR & Optimizer pick & Diff" + "\\\\\n"

        polr_min = min(results[mode][benchmark_name]["polr"]) * 1000
        std_min = min(results[mode][benchmark_name]["std"]) * 1000
        diff_min = polr_min / std_min * 100
        result_str += "Min & "
        if polr_min < std_min:
            result_str += "\\textbf{" + "{:10.2f}".format(polr_min) + "} &" + "{:10.2f}".format(std_min) + \
                          " & \\textbf{-" + "{:.2f}".format(100 - diff_min) + "\\%}\\\\\n"
        else:
            result_str += "{:10.2f}".format(polr_min) + " & \\textbf{" + "{:10.2f}".format(std_min) + \
                          "} & +" + "{:.2f}".format(diff_min - 100) + "\\%\\\\\n"

        polr_avg = mean(results[mode][benchmark_name]["polr"]) * 1000
        std_avg = mean(results[mode][benchmark_name]["std"]) * 1000
        diff_avg = polr_avg / std_avg * 100
        result_str += "Avg & "
        if polr_avg < std_avg:
            result_str += "\\textbf{" + "{:10.2f}".format(polr_avg) + "} &" + "{:10.2f}".format(std_avg) + \
                          " & \\textbf{-" + "{:.2f}".format(100 - diff_avg) + "\\%}\\\\\n"
        else:
            result_str += "{:10.2f}".format(polr_avg) + " & \\textbf{" + "{:10.2f}".format(std_avg) + \
                          "} & +" + "{:.2f}".format(diff_avg - 100) + "\\%\\\\\n"

        polr_95p = np.percentile(np.array(results[mode][benchmark_name]["polr"]), 95) * 1000
        std_95p = np.percentile(np.array(results[mode][benchmark_name]["std"]), 95) * 1000
        diff_95p = polr_95p / std_95p * 100
        result_str += "95p & "
        if polr_95p < std_95p:
            result_str += "\\textbf{" + "{:10.2f}".format(polr_95p) + "} &" + "{:10.2f}".format(std_95p) + \
                          " & \\textbf{-" + "{:.2f}".format(100 - diff_95p) + "\\%}\\\\\n"
        else:
            result_str += "{:10.2f}".format(polr_95p) + " & \\textbf{" + "{:10.2f}".format(std_95p) + \
                          "} & +" + "{:.2f}".format(diff_95p - 100) + "\\%\\\\\n"

        polr_max = max(results[mode][benchmark_name]["polr"]) * 1000
        std_max = max(results[mode][benchmark_name]["std"]) * 1000
        diff_max = polr_max / std_max * 100
        result_str += "Max & "
        if polr_max < std_max:
            result_str += "\\textbf{" + "{:10.2f}".format(polr_max) + "} &" + "{:10.2f}".format(std_max) + \
                          " & \\textbf{-" + "{:.2f}".format(100 - diff_max) + "\\%}\\\\\n"
        else:
            result_str += "{:10.2f}".format(polr_max) + " & \\textbf{" + "{:10.2f}".format(std_max) + \
                          "} & +" + "{:.2f}".format(diff_max - 100) + "\\%\\\\\n"

        polr_sum = sum(results[mode][benchmark_name]["polr"]) * 1000
        std_sum = sum(results[mode][benchmark_name]["std"]) * 1000
        diff_sum = polr_sum / std_sum * 100
        result_str += "Total & "
        if polr_sum < std_sum:
            result_str += "\\textbf{" + "{:10.2f}".format(polr_sum) + "} &" + "{:10.2f}".format(std_sum) + \
                          " & \\textbf{-" + "{:.2f}".format(100 - diff_sum) + "\\%}\\\\\n"
        else:
            result_str += "{:10.2f}".format(polr_sum) + " & \\textbf{" + "{:10.2f}".format(std_sum) + \
                          "} & +" + "{:.2f}".format(diff_sum - 100) + "\\%\\\\\n"

        result_str += "\\hline\\\\\n"
    result_str += "\\end{tabular}\n\n"

with open("experiment-results/03-performance.txt", "w") as file:
    file.write(result_str)