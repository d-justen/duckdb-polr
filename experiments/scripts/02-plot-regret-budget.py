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
regret_budgets = ["0-00001", "0-0001", "0-001", "0-01", "0-1", "0-2", "0-4", "0-8", "1-6", "2-4", "3-2"]

results = {}

for mode in modes:
    print("Processing " + mode)
    results[mode] = {}

    for benchmark_name in query_counts:
        opt_path = os.getcwd() + "/experiment-results/02-regret-budget/" + mode + "/" + benchmark_name + "/optimum"
        csv_files = glob.glob(os.path.join(opt_path, "*.csv"))
        csv_files.sort()

        opt_intms = []
        duckdb_intms = []
        opt_order_intms = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)

            # Cleaning
            df.pop(df.columns[-1])

            opt_intms.append(df.min(axis=1).sum())
            duckdb_intms.append(df["path_0"].sum())
            opt_order_intms.append(df.sum().min())

        routing_overheads = []

        all_polr_intms = {}
        for regret_budget in regret_budgets:
            polr_intms = []

            path = os.getcwd() + "/experiment-results/02-regret-budget/" + \
                   mode + "/" + benchmark_name + "/" + regret_budget
            txt_files = glob.glob(os.path.join(path, "*.txt"))
            txt_files.sort()

            for txt_file in txt_files:
                with open(txt_file) as f:
                    line = f.readline()
                    polr_intms.append(int(line))

            all_polr_intms[regret_budget] = polr_intms
            routing_overheads.append(sum(polr_intms) / sum(opt_intms))

        optimizer_pick = sum(duckdb_intms) / sum(opt_intms)
        optimal_order = sum(opt_order_intms) / sum(opt_intms)

        relative_overhead_per_query = []
        for i in range(len(duckdb_intms)):
            if opt_intms[i] > 0:
                relative_overhead_per_query.append(all_polr_intms["0-01"][i] / opt_intms[i])
            else:
                relative_overhead_per_query.append(all_polr_intms["0-01"][i] * -1)

        print(mode + " " + benchmark_name + ":")
        print(relative_overhead_per_query)
        results[mode][benchmark_name] = {"optimizer": optimizer_pick, "polr": routing_overheads,
                                         "optimal_order": optimal_order}
        print(results)

print(results)

min_optimizer_overhead_job = results[modes[0]]["job"]["optimizer"]
min_optimizer_overhead_ssb = results[modes[0]]["ssb"]["optimizer"]
min_optimal_overhead_job = results[modes[0]]["job"]["optimal_order"]
min_optimal_overhead_ssb = results[modes[0]]["ssb"]["optimal_order"]

for mode in modes:
    if results[mode]["job"]["optimizer"] < min_optimizer_overhead_job:
        min_optimizer_overhead_job = results[mode]["job"]["optimizer"]
    if results[mode]["ssb"]["optimizer"] < min_optimizer_overhead_ssb:
        min_optimizer_overhead_ssb = results[mode]["ssb"]["optimizer"]
    if results[mode]["job"]["optimal_order"] < min_optimal_overhead_job:
        min_optimal_overhead_job = results[mode]["job"]["optimal_order"]
    if results[mode]["ssb"]["optimal_order"] < min_optimal_overhead_ssb:
        min_optimal_overhead_ssb = results[mode]["ssb"]["optimal_order"]

x_values = []
baseline = []
for regret_budget in regret_budgets:
    x_values.append(float(regret_budget.replace("-", ".")))
    baseline.append(1)

optimizer_baseline_job = [min_optimizer_overhead_job] * len(baseline)
optimizer_baseline_ssb = [min_optimizer_overhead_ssb] * len(baseline)
optimal_order_baseline_job = [min_optimal_overhead_job] * len(baseline)
optimal_order_baseline_ssb = [min_optimal_overhead_ssb] * len(baseline)

fig, ax = plt.subplots(2, 1)

ax[0].plot(x_values, baseline, "-.")
ax[0].plot(x_values, results["dphyp-equisets"]["job"]["polr"])
ax[0].plot(x_values, results["dphyp-constant"]["job"]["polr"])
ax[0].plot(x_values, results["greedy-equisets"]["job"]["polr"])
ax[0].plot(x_values, results["greedy-constant"]["job"]["polr"])
ax[0].plot(x_values, optimizer_baseline_job, "-.")
ax[0].plot(x_values, optimal_order_baseline_job, "-.")
ax[0].set_xlabel("Regret budget")
ax[0].set_ylabel("Intermediate overhead")
ax[0].set_xscale("log")
ax[0].yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
ax[0].set_title("Join order benchmark")
box0 = ax[0].get_position()
ax[0].set_position([box0.x0, box0.y0 + 0.05, box0.width * 0.75, box0.height])

ax[1].plot(x_values, baseline, "-.")
ax[1].plot(x_values, results["dphyp-equisets"]["ssb"]["polr"])
ax[1].plot(x_values, results["dphyp-constant"]["ssb"]["polr"])
ax[1].plot(x_values, results["greedy-equisets"]["ssb"]["polr"])
ax[1].plot(x_values, results["greedy-constant"]["ssb"]["polr"])
ax[1].plot(x_values, optimizer_baseline_ssb, "-.")
ax[1].plot(x_values, optimal_order_baseline_ssb, "-.")
ax[1].set_xlabel("Regret budget")
ax[1].set_ylabel("Intermediate overhead")
ax[1].set_xscale("log")
ax[1].yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
ax[1].set_title("Star-schema benchmark")
box1 = ax[1].get_position()
ax[1].set_position([box1.x0, box1.y0 - 0.05, box1.width * 0.75, box1.height])

ax[0].legend(["optimal switches", "dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant",
               "POLAR disabled", "optimal join order"], loc="center left", bbox_to_anchor=(1, 0.5))
# plt.tight_layout()
plt.savefig("experiment-results/02-regret-budget.pdf")