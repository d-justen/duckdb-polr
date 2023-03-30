#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
from statistics import mean

modes = ["dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"]
query_counts = {"job": 113, "tpch": 22, "ssb": 12}
results = {}

for mode in modes:
    results[mode] = {"polr_amounts": [], "avg_path_counts": []}
    for benchmark_name in query_counts:
        path = os.getcwd() + "/experiment-results/00-num-join-paths/" + mode + "/" + benchmark_name
        print("Processing " + benchmark_name)
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        csv_files.sort()

        # path_counts = [0] * (query_counts[benchmark_name] - len(csv_files))
        path_counts = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            num_paths = int(len(df.columns) / 2)
            path_counts.append(num_paths)

        polr_amount = len(path_counts) / query_counts[benchmark_name]
        avg_path_count = mean(path_counts)
        results[mode]["polr_amounts"].append(polr_amount)
        results[mode]["avg_path_counts"].append(avg_path_count)
print(results)

groups = list(query_counts.keys())

bar_width = 0.2
fig, ax = plt.subplots(1, 2)
fig.set_size_inches(8, 4)

br1 = np.arange(len(results[modes[0]]["polr_amounts"]))
br2 = [x + bar_width for x in br1]
br3 = [x + bar_width for x in br2]
br4 = [x + bar_width for x in br3]

bar1 = ax[0].bar(br1, results["dphyp-equisets"]["polr_amounts"], width=bar_width, label="dpeq")
bar2 = ax[0].bar(br2, results["dphyp-constant"]["polr_amounts"], width=bar_width, label="dpco")
bar3 = ax[0].bar(br3, results["greedy-equisets"]["polr_amounts"], width=bar_width, label="greq")
bar4 = ax[0].bar(br4, results["greedy-constant"]["polr_amounts"], width=bar_width, label="grco")

ax[0].set_ylabel("# queries with POLAR pipeline")
ax[0].set_xticks([r + 1.5 * bar_width for r in range(len(groups))], groups)
ax[0].yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

ax[1].bar(br1, results["dphyp-equisets"]["avg_path_counts"], width=bar_width, label="dpeq")
ax[1].bar(br2, results["dphyp-constant"]["avg_path_counts"], width=bar_width, label="dpco")
ax[1].bar(br3, results["greedy-equisets"]["avg_path_counts"], width=bar_width, label="greq")
ax[1].bar(br4, results["greedy-constant"]["avg_path_counts"], width=bar_width, label="grco")

ax[1].set_ylabel("Average path count per POLAR pipeline")
ax[1].set_xticks([r + 1.5 * bar_width for r in range(len(groups))], groups)

box = ax[0].get_position()
ax[0].set_position([box.x0, box.y0 + box.height * 0.05,
                 box.width, box.height * 0.95])

box2 = ax[1].get_position()
ax[1].set_position([box2.x0, box2.y0 + box2.height * 0.05,
                    box2.width, box2.height * 0.95])

# Put a legend below current axis
plt.figlegend([bar1, bar2, bar3, bar4], ["dpeq", "dpco", "greq", "grco"], loc="lower center", ncol=4)
plt.savefig("experiment-results/00-num-join-paths.pdf")
