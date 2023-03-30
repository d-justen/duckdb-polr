#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
from statistics import mean

query_counts = {"job": 113, "ssb": 12}
modes = ["dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"]
regret_budgets = ["0-0001", "0-001", "0-01", "0-1", "0-2", "0-4", "0-8", "1-6", "2-4", "3-2"]

results = {}

for mode in modes:
    print("Processing " + mode)
    results[mode] = {}

    for benchmark_name in query_counts:
        opt_path = os.getcwd() + "/experiment-results/02-regret-budget/" + mode + "/" + benchmark_name + "/optimum"
        csv_files = glob.glob(os.path.join(opt_path, "*.csv"))
        csv_files.sort()

        opt_intm_count = 0
        duckdb_intm_count = 0

        skiplist = []
        idx = 0
        for csv_file in csv_files:
            idx += 1
            df = pd.read_csv(csv_file)

            # Cleaning
            df.pop(df.columns[-1])
            if df.shape[0] > df.shape[1] / 2:
                df = df.iloc[int(df.shape[1] / 2) - 1:]
                df.reset_index(drop=True, inplace=True)
            else:
                skiplist.append(idx)
                continue

            tuples_sent = df.drop(df.columns[list(range(0, df.shape[1], 2))], axis=1)
            ratios = df.drop(df.columns[list(range(1, df.shape[1], 2))], axis=1)
            df["input_tuple_count"] = tuples_sent.sum(axis=1)
            df["diff_input_tuple_count"] = df["input_tuple_count"].diff()
            df.at[0, "diff_input_tuple_count"] = df.at[0, "input_tuple_count"]
            df["polr"] = ratios.min(axis=1)
            df["intms_polr_opt"] = (df["polr"]) * df["diff_input_tuple_count"]
            df["intms_duckdb"] = (df["intermediates_0"]) * df["diff_input_tuple_count"]

            opt_intm_count += df["intms_polr_opt"].sum()
            duckdb_intm_count += df["intms_duckdb"].sum()

        routing_overheads = []

        for regret_budget in regret_budgets:
            intms_polr = 0

            path = os.getcwd() + "/experiment-results/02-regret-budget/" + \
                   mode + "/" + benchmark_name + "/" + regret_budget
            txt_files = glob.glob(os.path.join(path, "*.txt"))
            txt_files.sort()

            idx = 0
            for txt_file in txt_files:
                idx += 1
                with open(txt_file) as f:
                    if idx in skiplist:
                        continue
                    line = f.readline()
                    intms_polr += int(line)

            routing_overheads.append(intms_polr / opt_intm_count)

        optimizer_pick = duckdb_intm_count / opt_intm_count
        results[mode][benchmark_name] = {"optimizer": optimizer_pick, "polr": routing_overheads}

print(results)

x_values = []
baseline = []
for regret_budget in regret_budgets:
    x_values.append(float(regret_budget.replace("-", ".")))
    baseline.append(1)

fig, ax = plt.subplots(2, 1)

ax[0].plot(x_values, baseline, "-.")
ax[0].plot(x_values, results["dphyp-equisets"]["job"]["polr"])
ax[0].plot(x_values, results["dphyp-constant"]["job"]["polr"])
ax[0].plot(x_values, results["greedy-equisets"]["job"]["polr"])
ax[0].plot(x_values, results["greedy-constant"]["job"]["polr"])
ax[0].set_xlabel("Regret budget")
ax[0].set_ylabel("Intermediate overhead")
ax[0].set_xscale("log")
ax[0].yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax[0].set_title("Join order benchmark")

ax[1].plot(x_values, baseline, "-.")
ax[1].plot(x_values, results["dphyp-equisets"]["ssb"]["polr"])
ax[1].plot(x_values, results["dphyp-constant"]["ssb"]["polr"])
ax[1].plot(x_values, results["greedy-equisets"]["ssb"]["polr"])
ax[1].plot(x_values, results["greedy-constant"]["ssb"]["polr"])
ax[1].set_xlabel("Regret budget")
ax[1].set_ylabel("Intermediate overhead")
ax[1].set_xscale("log")
ax[1].yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax[1].set_title("Star-schema benchmark")

plt.figlegend(["optimum", "dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"], loc="center right")
plt.tight_layout()
plt.savefig("experiment-results/02-regret-budget.pdf")