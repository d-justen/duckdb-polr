#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

query_counts = {"job": 113, "tpch": 22, "ssb": 12}
results = {}

for benchmark_name in query_counts:
    path = os.getcwd() + "/experiment-results/01-num-intermediates/" + benchmark_name
    print("Processing " + benchmark_name)
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    csv_files.sort()

    relative_savings = []
    absolute_savings = []
    savings = {"rel": [], "abs": []}

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)

        # Cleaning
        df.pop(df.columns[-1])
        if df.shape[0] > df.shape[1] / 2:
            df = df.iloc[int(df.shape[1] / 2)-1:]
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
            savings["abs"].append(num_intms_duckdb - num_intms_polr_opt)
            savings["rel"].append(1 - (num_intms_polr_opt / num_intms_duckdb))

    results[benchmark_name] = savings

fig, ax = plt.subplots(2, 2)

ax[0, 0].bar(range(len(results["job"]["rel"])), results["job"]["rel"])
ax[0, 0].set_title("job relative")
ax[0, 1].bar(range(len(results["ssb"]["rel"])), results["ssb"]["rel"])
ax[0, 1].set_title("ssb relative")
bottomlim, toplim = ax[0, 0].get_ylim()
ax[0, 1].set_ylim(top=toplim)
ax[1, 0].bar(range(len(results["job"]["abs"])), results["job"]["abs"])
ax[1, 0].set_title("job absolute")
ax[1, 1].bar(range(len(results["ssb"]["abs"])), results["ssb"]["abs"])
ax[1, 1].set_title("ssb absolute")
bottomlim2, toplim2 = ax[1, 0].get_ylim()
ax[1, 1].set_ylim(top=toplim2)

plt.tight_layout()
plt.savefig("experiment-results/01-num-intermediates.pdf")
