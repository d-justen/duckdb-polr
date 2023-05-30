#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

query_counts = {"job": 113, "ssb": 12}
static_routing_strategies = ["std", "default_path", "init_once", "opportunistic"]
adaptive_routing_strategies = ["adaptive_reinit", "dynamic"]
regret_budgets = ["0.00001", "0.0001", "0.001", "0.01", "0.1", "0.2", "0.4", "0.8", "1.6", "2.4", "3.2"]
modes = ["dphyp-equisets", "dphyp-constant"]
results = {}

for mode in modes:
    results[mode] = {}
    for benchmark_name in query_counts:
        results[mode][benchmark_name] = {}
        path = os.getcwd() + "/experiment-results/05-performance-per-regret/" + mode + "/" + benchmark_name

        for strategy in static_routing_strategies:
            files = glob.glob(os.path.join(path + "/" + strategy, "*.csv"))
            if len(files) == 0:
                continue
            df = pd.read_csv(files[0], names=["name", "run", "timing"])
            results[mode][benchmark_name][strategy] = float(df.groupby("name").mean()["timing"].sum())

        for strategy in adaptive_routing_strategies:
            results[mode][benchmark_name][strategy] = []
            for regret_budget in regret_budgets:
                file_name = strategy + "-" + regret_budget + ".csv"
                files = glob.glob(os.path.join(path + "/" + strategy + "/" + file_name))
                if len(files) == 0:
                    continue
                df = pd.read_csv(files[0], names=["name", "run", "timing"])
                results[mode][benchmark_name][strategy].append(float(df.groupby("name").mean()["timing"].sum()))

print(results)

x_values = []
for regret_budget in regret_budgets:
    x_values.append(float(regret_budget.replace("-", ".")))

for benchmark_name in query_counts:
    fig, ax = plt.subplots(1, len(modes))
    fig.set_size_inches(12, 4)
    i = 0
    for mode in modes:
        result = results[mode][benchmark_name]
        for strategy in static_routing_strategies:
            if strategy in result:
                ax[i].plot(x_values, [result[strategy]] * len(regret_budgets))
            else:
                ax[i].plot(x_values, [0] * len(regret_budgets))
        for strategy in adaptive_routing_strategies:
            if len(result[strategy]) == len (regret_budgets):
                ax[i].plot(x_values, result[strategy])
            else:
                ax[i].plot(x_values, [0] * len(regret_budgets))
        ax[i].set_ylim(bottom=0)
        ax[i].set_xscale("log")
        ax[i].set_xlabel("Regret Budget")
        ax[i].set_title(mode.split("-")[0][:2] + mode.split("-")[1][:2])
        i += 1
    ax[0].set_ylabel("Total execution time")
    plt.legend(["std", "default_path", "init_once", "opportunistic", "adaptive_reinit", "dynamic"])
    plt.tight_layout()
    plt.savefig("experiment-results/05-performance-per-regret-" + benchmark_name + ".pdf")
    plt.clf()
