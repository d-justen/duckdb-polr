#!/usr/bin/env python3

import pandas as pd
import os
import glob
from statistics import mean
import matplotlib.pyplot as plt

enumerators = ["each_last_once", "each_first_once", "dfs_random", "dfs_min_card", "dfs_uncertain", "bfs_random",
               "bfs_min_card", "bfs_uncertain"]

results = {}

for enumerator in enumerators:
    path = os.getcwd() + "/experiment-results/1_2_sel_dur/greedy-equisets-ldt/imdb/" + enumerator
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    csv_files.sort()

    enumeration_times = {}
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        num_joins = df["num_joins"].min()
        duration = df["enumeration_time_ms"].min()

        if num_joins not in enumeration_times:
            enumeration_times[num_joins] = []

        enumeration_times[num_joins].append(duration)

    results[enumerator] = {}
    for num_joins in enumeration_times:
        results[enumerator][num_joins] = mean(enumeration_times[num_joins])

print(results)


x_values = []
for enumerator in enumerators:
    if len(results[enumerator]) > 0:
        x_values = sorted(list(results[enumerator].keys()))
        break

for enumerator in enumerators:
    if len(results[enumerator]) == 0:
        continue

    y_values = []
    for num_joins in sorted(results[enumerator].keys()):
        y_values.append(results[enumerator][num_joins])

    plt.plot(x_values, y_values, label=enumerator)

plt.xlabel("Number of joins in pipeline")
plt.ylabel("Join order generation time (ms)")
plt.ylim(bottom=0)
plt.legend()
plt.xticks(ticks=x_values, labels=x_values)

plt.savefig("experiment-results/1_2_sel_dur.pdf")
