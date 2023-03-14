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
    path = os.getcwd() + "/experiment-results/00-num-join-paths/" + benchmark_name
    print("Processing " + benchmark_name)
    csv_files = glob.glob(os.path.join(path, "*.csv"))

    path_counts = [0] * (query_counts[benchmark_name] - len(csv_files))

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        num_paths = int(len(df.columns) / 2)
        path_counts.append(num_paths)

    results[benchmark_name] = path_counts

max_paths = 0
for benchmark_name in results:
    maximum = max(results[benchmark_name])
    if maximum > max_paths:
        max_paths = maximum

groups = list(query_counts.keys())
values = [list() for _ in range(max_paths + 1)]
for benchmark_name in results:
    for i in range(max_paths + 1):
        amount = results[benchmark_name].count(i) / query_counts[benchmark_name]
        values[i].append(amount)

path_counts = []
for i in reversed(range(max_paths + 1)):
    if sum(values[i]) == 0:
        del values[i]
    else:
        path_counts.append(i)

path_counts.reverse()
np_values = np.array(values)

fig, ax = plt.subplots()
for i in range(np_values.shape[0]):
    ax.bar(groups, np_values[i], bottom=np.sum(np_values[:i], axis=0), label=str(path_counts[i]))

for bar in ax.patches:
    if bar.get_height() > 0.05:
        ax.text(bar.get_x() + bar.get_width() / 2,
                (bar.get_height() / 2 + bar.get_y()) - 0.015,
                f"{round(bar.get_height(), 3):.1%}", ha = 'center',
                color = 'w', weight = "bold", size = 8)

ax.set_ylabel("join paths generated in benchmark queries")
ax.yaxis.set_major_formatter(mtick.PercentFormatter())

box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.1,
                 box.width, box.height * 0.9])

ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.08),
          ncol=len(path_counts))

plt.savefig("experiment-results/00-num-join-paths.pdf")
