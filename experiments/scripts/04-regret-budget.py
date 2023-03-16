#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

query_counts = {"job": 113, "ssb": 22}
results = {}

for benchmark_name in query_counts:
    path = os.getcwd() + "/experiment-results/04-regret-budget/" + benchmark_name
    print("Processing " + benchmark_name)
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    csv_files.sort()

    labels = []
    tets = []
    maxs = []

    for csv_file in csv_files:
        file_name = csv_file.split("/")[-1].split(".")[0]
        df = pd.read_csv(csv_file, names=["name", "run", "timing"])
        df = df.groupby("name").mean()

        labels.append(float(file_name.replace("_", ".")))
        tets.append(df["timing"].sum())
        maxs.append(df["timing"].max())

    results[benchmark_name] = {"labels": labels, "tets": tets, "maxs": maxs}

fig, ax = plt.subplots(2, 2)

ax[0, 0].plot(results["job"]["labels"], results["job"]["tets"])
ax[0, 0].set_title("[job] TET per regret budget")
ax[0, 1].plot(results["job"]["labels"], results["job"]["maxs"])
ax[0, 1].set_title("[job] max dur. per regret budget")
ax[1, 0].plot(results["ssb"]["labels"], results["ssb"]["tets"])
ax[1, 0].set_title("[ssb] TET per regret budget")
ax[1, 1].plot(results["ssb"]["labels"], results["ssb"]["maxs"])
ax[1, 1].set_title("[ssb] max dur. per regret budget")

ax[0, 0].set_xscale("log")
ax[0, 1].set_xscale("log")
ax[1, 0].set_xscale("log")
ax[1, 1].set_xscale("log")

plt.tight_layout()
plt.savefig("experiment-results/04-regret-budget.pdf")
