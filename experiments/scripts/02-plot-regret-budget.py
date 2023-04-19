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
static_routing_strategies = ["init_once", "opportunistic"]
adaptive_routing_strategies = ["adaptive_reinit", "dynamic"]
regret_budgets = ["0.00001", "0.0001", "0.001", "0.01", "0.1", "0.2", "0.4", "0.8", "1.6", "2.4", "3.2"]

results = {}

for mode in modes:
    print("Processing " + mode)
    results[mode] = {}

    for benchmark_name in query_counts:
        strategy = "alternate"

        opt_path = os.getcwd() + "/experiment-results/02-regret-budget/" + mode + "/" + strategy + "/" + benchmark_name
        csv_files = glob.glob(os.path.join(opt_path, "*.csv"))
        csv_files.sort()

        opt_intms = []
        duckdb_intms = []
        opt_order_intms = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)

            # Cleaning
            df.pop(df.columns[-1])

            if not "path_0" in df.columns:
                opt_intms.append(0)
                duckdb_intms.append(0)
                opt_intms.append(0)
                continue

            opt_intms.append(df.min(axis=1).sum())
            duckdb_intms.append(df["path_0"].sum())
            opt_order_intms.append(df.sum().min())

        benchmark_result = {"optimum": sum(opt_intms), "default": sum(duckdb_intms), "optimal_order": sum(opt_order_intms)}

        for strategy in static_routing_strategies:
            path = os.getcwd() + "/experiment-results/02-regret-budget/" + mode + "/" + strategy + "/" + benchmark_name
            txt_files = glob.glob(os.path.join(path, "*.txt"))
            txt_files.sort()

            intms = []
            for txt_file in txt_files:
                with open(txt_file) as f:
                    line = f.readline()
                    if line == "":
                        intms.append(0)
                        continue
                    intms.append(int(line))

            benchmark_result[strategy] = sum(intms)

        for strategy in adaptive_routing_strategies:
            benchmark_result[strategy] = {}
            for regret_budget in regret_budgets:
                path = os.getcwd() + "/experiment-results/02-regret-budget/" + mode + "/" + strategy + "/" + \
                       benchmark_name + "/" + regret_budget
                txt_files = glob.glob(os.path.join(path, "*.txt"))
                txt_files.sort()

                intms = []
                for txt_file in txt_files:
                    with open(txt_file) as f:
                        line = f.readline()
                        if line == "":
                            intms.append(0)
                            continue
                        intms.append(int(line))

                benchmark_result[strategy][regret_budget] = sum(intms)

        results[mode][benchmark_name] = benchmark_result

print(results)

x_values = []
for regret_budget in regret_budgets:
    x_values.append(float(regret_budget.replace("-", ".")))

for mode in modes:
    for benchmark_name in query_counts:
        result = results[mode][benchmark_name]
        plt.plot(x_values, [result["optimum"]] * len(regret_budgets), "-.")
        plt.plot(x_values, [result["optimal_order"]] * len(regret_budgets), "-.")
        plt.plot(x_values, [result["default"]] * len(regret_budgets), "-.")
        plt.plot(x_values, [result["init_once"]] * len(regret_budgets))
        plt.plot(x_values, [result["opportunistic"]] * len(regret_budgets))
        plt.plot(x_values, list(result["adaptive_reinit"].values()))
        plt.plot(x_values, list(result["dynamic"].values()))
        plt.xscale("log")
        plt.xlabel("Regret Budget")
        plt.ylabel("Intermediate count")
        plt.legend(["opt", "opt_order", "def_order", "init_once", "opportunistic", "adaptive_reinit", "dynamic"])
        plt.tight_layout()
        plt.savefig("experiment-results/02-regret-budget-" + mode + "-" + benchmark_name + ".pdf")
        plt.clf()
