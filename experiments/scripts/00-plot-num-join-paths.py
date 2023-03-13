#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

query_counts = {"job": 113, "tpch": 22, "ssb": 12}
results = {}

for benchmark_name in query_counts: # TODO query_counts.keys()
    path = os.getcwd() + "/experiment-results/00-num-join-paths/" + benchmark_name
    print(path)
    csv_files = glob.glob(os.path.join(path, "*.csv"))

    path_counts = [0] * (query_counts[benchmark_name] - len(csv_files))

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        num_paths = int(len(df.columns) / 2)
        path_counts.append(num_paths)

    results[benchmark_name] = path_counts

print(results)
fig = plt.figure(figsize=(10, 4))
data = [results["job"], results["tpch"], results["ssb"]]
plt.boxplot(data, showmeans=True, labels=["job", "tpch", "ssb"])
plt.savefig("experiment-results/00-num-join-paths.pdf", bbox_inches="tight")