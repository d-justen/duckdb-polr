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

for mode in modes:
    results[mode] = {}

    for benchmark_name in query_counts:
        path = os.getcwd() + "/experiment-results/01-num-intermediates/" + mode + "/" + benchmark_name
        print("Processing " + benchmark_name)
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        csv_files.sort()

        savings = []
        intms_duckdb = []
        intms_polr = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)

            # Cleaning
            df.pop(df.columns[-1])
            if df.shape[0] > df.shape[1] / 2:
                df = df.iloc[int(df.shape[1] / 2) - 1:]
                df.reset_index(drop=True, inplace=True)
            else:
                continue

            tuples_sent = df.drop(df.columns[list(range(0, df.shape[1], 2))], axis=1)
            ratios = df.drop(df.columns[list(range(1, df.shape[1], 2))], axis=1)
            df["input_tuple_count"] = tuples_sent.sum(axis=1)
            df["diff_input_tuple_count"] = df["input_tuple_count"].diff()
            df.at[0, "diff_input_tuple_count"] = df.at[0, "input_tuple_count"]
            df["polr"] = ratios.min(axis=1)
            df["intms_duckdb"] = (df["intermediates_0"]) * df["diff_input_tuple_count"]
            df["intms_polr_opt"] = (df["polr"]) * df["diff_input_tuple_count"]

            num_intms_duckdb = df["intms_duckdb"].sum()
            num_intms_polr_opt = df["intms_polr_opt"].sum()

            if num_intms_duckdb > 0:
                savings.append(1 - (num_intms_polr_opt / num_intms_duckdb))
                intms_duckdb.append(num_intms_duckdb)
                intms_polr.append(num_intms_polr_opt)

        results[mode][benchmark_name] = 1 - sum(intms_polr) / sum(intms_duckdb)

experiment_output = "config,"
for benchmark_name in query_counts:
    experiment_output += benchmark_name + ","
experiment_output = experiment_output[:-1] + "\n"

for mode in results:
    experiment_output += mode + ","
    for benchmark_name in query_counts:
        experiment_output += "{0:.0%}".format(results[mode][benchmark_name]) + ","
    experiment_output = experiment_output[:-1] + "\n"

with open("experiment-results/01-num-intermediates.csv", "w") as file:
    file.write(experiment_output)
