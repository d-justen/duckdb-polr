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
    path = os.getcwd() + "/experiment-results/03-performance/" + benchmark_name
    print("Processing " + benchmark_name)
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    csv_files.sort()

    timings = {}

    for csv_file in csv_files:
        file_name = csv_file.split("/")[-1].split(".")[0]
        df = pd.read_csv(csv_file, names=["name", "run", "timing"])
        df = df.groupby("name").mean()

        timings[file_name] = df["timing"].values.tolist()
        print(file_name + ": " + str(df["timing"].sum()) + "s")

    results[benchmark_name] = timings

fig, ax = plt.subplots(3, 1)

bar_width = 0.25

ax0_br1 = np.arange(len(results["job"]["job-polr"]))
ax0_br2 = [x + bar_width for x in ax0_br1]
bar1 = ax[0].bar(ax0_br1, results["job"]["job-polr"], width=bar_width)
bar2 = ax[0].bar(ax0_br2, results["job"]["job"], width=bar_width)
ax[0].set_xticks([r + (bar_width / 2) for r in range(len(results["job"]["job-polr"]))])
ax[0].set_xticklabels(range(len(results["job"]["job-polr"])), fontsize=4)
ax[0].set_title("job")

ax1_br1 = np.arange(len(results["tpch"]["tpch-polr"]))
ax1_br2 = [x + bar_width for x in ax1_br1]
ax[1].bar(ax1_br1, results["tpch"]["tpch-polr"], width=bar_width)
ax[1].bar(ax1_br2, results["tpch"]["tpch"], width=bar_width)
ax[1].set_xticks([r + (bar_width / 2) for r in range(len(results["tpch"]["tpch-polr"]))])
ax[1].set_xticklabels(range(len(results["tpch"]["tpch-polr"])))
ax[1].set_title("tpch")

ax2_br1 = np.arange(len(results["ssb"]["ssb-polr"]))
ax2_br2 = [x + bar_width for x in ax2_br1]
ax[2].bar(ax2_br1, results["ssb"]["ssb-polr"], width=bar_width)
ax[2].bar(ax2_br2, results["ssb"]["ssb"], width=bar_width)
ax[2].set_xticks([r + (bar_width / 2) for r in range(len(results["ssb"]["ssb-polr"]))])
ax[2].set_xticklabels(range(len(results["ssb"]["ssb-polr"])))
ax[2].set_title("ssb")

plt.tight_layout()
plt.figlegend([bar1, bar2], ["polr", "duckdb"])
plt.savefig("experiment-results/03-performance.pdf")
