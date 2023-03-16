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
    path = os.getcwd() + "/experiment-results/02-routing-efficacy/" + benchmark_name
    print("Processing " + benchmark_name)
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    csv_files.sort()

    intermediates = {"polr_opt": [], "polr": [], "duckdb": []}
    skip_list = []

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)

        # Cleaning
        df.pop(df.columns[-1])
        if df.shape[0] > df.shape[1] / 2:
            df = df.iloc[int(df.shape[1] / 2)-1:]
            df.reset_index(drop=True, inplace=True)
        else:
            file_name = csv_file.split("/")[-1].split(".")[0]
            skip_list.append(file_name)
            continue

        tuples_sent = df.drop(df.columns[list(range(0, df.shape[1], 2))], axis=1)
        ratios = df.drop(df.columns[list(range(1, df.shape[1], 2))], axis=1)
        df["input_tuple_count"] = tuples_sent.sum(axis=1)
        df["diff_input_tuple_count"] = df["input_tuple_count"].diff()
        df.at[0, "diff_input_tuple_count"] = df.at[0, "input_tuple_count"]
        df["polr"] = ratios.min(axis=1)
        df["intms_polr_opt"] = (df["polr"]) * df["diff_input_tuple_count"]
        df["intms_duckdb"] = (df["intermediates_0"]) * df["diff_input_tuple_count"]

        num_intms_polr_opt = df["intms_polr_opt"].sum()
        num_intms_duckdb = df["intms_duckdb"].sum()

        intermediates["polr_opt"].append(num_intms_polr_opt)
        intermediates["duckdb"].append(num_intms_duckdb)

    txt_files = glob.glob(os.path.join(path, "*.txt"))
    txt_files.sort()

    for txt_file in txt_files:
        with open(txt_file) as f:
            file_name = csv_file.split("/")[-1].split(".")[0]
            # TODO: skip if contained in skiplist
            line = f.readline()
            intermediates["polr"].append(int(line))

    results[benchmark_name] = intermediates

fig, ax = plt.subplots(3, 1)
bar_width = 0.25

ax0_br1 = np.arange(len(results["job"]["polr_opt"]))
ax0_br2 = [x + bar_width for x in ax0_br1]
ax0_br3 = [x + bar_width for x in ax0_br2]

bar1 = ax[0].bar(ax0_br1, results["job"]["polr_opt"], width=bar_width)
bar2 = ax[0].bar(ax0_br2, results["job"]["polr"], width=bar_width)
bar3 = ax[0].bar(ax0_br3, results["job"]["duckdb"], width=bar_width)
ax[0].set_xticks([r + bar_width for r in range(len(results["job"]["polr_opt"]))])
ax[0].set_xticklabels(range(len(results["job"]["polr_opt"])), fontsize=4)
ax[0].set_yscale("log")
ax[0].set_title("job")

ax1_br1 = np.arange(len(results["tpch"]["polr_opt"]))
ax1_br2 = [x + bar_width for x in ax1_br1]
ax1_br3 = [x + bar_width for x in ax1_br2]

ax[1].bar(ax1_br1, results["tpch"]["polr_opt"], width=bar_width)
ax[1].bar(ax1_br2, results["tpch"]["polr"], width=bar_width)
ax[1].bar(ax1_br3, results["tpch"]["duckdb"], width=bar_width)
ax[1].set_xticks([r + bar_width for r in range(len(results["tpch"]["polr_opt"]))])
ax[1].set_xticklabels(range(len(results["tpch"]["polr_opt"])))
ax[1].set_yscale("log")
ax[1].set_title("tpch")

ax2_br1 = np.arange(len(results["ssb"]["polr_opt"]))
ax2_br2 = [x + bar_width for x in ax2_br1]
ax2_br3 = [x + bar_width for x in ax2_br2]

ax[2].bar(ax2_br1, results["ssb"]["polr_opt"], width=bar_width)
ax[2].bar(ax2_br2, results["ssb"]["polr"], width=bar_width)
ax[2].bar(ax2_br3, results["ssb"]["duckdb"], width=bar_width)
ax[2].set_xticks([r + bar_width for r in range(len(results["ssb"]["polr_opt"]))])
ax[2].set_xticklabels(range(len(results["ssb"]["polr_opt"])))
ax[2].set_yscale("log")
ax[2].set_title("ssb")

plt.figlegend([bar1, bar2, bar3], ["polr_opt", "polr", "duckdb"])
plt.tight_layout()
plt.savefig("experiment-results/02-routing-efficacy.pdf")
