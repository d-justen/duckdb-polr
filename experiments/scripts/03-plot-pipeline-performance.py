#!/usr/bin/env python3

import pandas as pd
import os
import glob
from statistics import mean
import numpy as np

query_counts = {"job": 113, "ssb": 12}
modes = ["dphyp-equisets", "dphyp-constant", "greedy-equisets", "greedy-constant"]
routing_strategies = ["dynamic", "adaptive_reinit", "init_once", "opportunistic", "std"]
results = {}

for mode in modes:
    results[mode] = {}

    for strategy in routing_strategies:
        results[mode][strategy] = {}

        for benchmark_name in query_counts:
            path = os.getcwd() + "/experiment-results/03-pipeline-performance/" + mode + "/" + strategy + "/" + benchmark_name

            polr_durations = glob.glob(os.path.join(path, "*.csv"))
            polr_durations.sort()

            polr_results = []
            benchmark_ids = []

            for file in polr_durations:
                benchmark_id = file.split("/")[-1].split(".")[0]
                benchmark_ids.append(benchmark_id)
                df = pd.read_csv(file, names=["timing"])
                polr_results.append(float(df.mean()["timing"]))

            results[mode][strategy][benchmark_name] = polr_results

print(results)

for mode in modes:
    for strategy in routing_strategies:
        for benchmark_name in query_counts:
            if len(results[mode][strategy][benchmark_name]) == 0:
                continue
            print("\n### " + mode + " " + strategy + " " + benchmark_name + " ###")
            rel_results = []
            for i in range(len(results[mode]["std"][benchmark_name])):
                rel = results[mode][strategy][benchmark_name][i] / results[mode]["std"][benchmark_name][i] - 1
                rel_results.append(rel)
                print(str(i) + ": " + "{:1.4f}".format(rel) + " | " + "{:1.2f}".format(results[mode][strategy][benchmark_name][i]) + " vs. " + "{:1.2f}".format(results[mode]["std"][benchmark_name][i]))

            if len(rel_results) > 0:
                print("Max Gain: " + str(min(rel_results)))
                print("Median Gain: " + str(mean(rel_results)))
                print("Max Overhead: " + str(max(rel_results)))
                print("Std Total: " + str(sum(results[mode]["std"][benchmark_name])) + " POLAR Total: " + str(sum(results[mode][strategy][benchmark_name])))
                print("Rel: " + "{:1.4f}".format(sum(results[mode][strategy][benchmark_name]) / sum(results[mode]["std"][benchmark_name]) - 1))

for mode in modes:
    for benchmark_name in query_counts:
        print("\n### " + mode + " " + benchmark_name + " ###")
        winners_per_query = {"dynamic": 0, "adaptive_reinit": 0, "init_once": 0, "opportunistic": 0, "std": 0, "same": 0}
        for i in range(len(results[mode]["std"][benchmark_name])):
            min_duration = 1000
            winner = ""

            for strategy in routing_strategies:
                if results[mode][strategy][benchmark_name][i] < min_duration:
                    min_duration = results[mode][strategy][benchmark_name][i]
                    winner = strategy

            if winner == "std":
                min_duration_2 = 1000
                winner_2 = ""

                for strategy in routing_strategies:
                    if strategy == "std":
                        continue
                    if results[mode][strategy][benchmark_name][i] < min_duration:
                        min_duration = results[mode][strategy][benchmark_name][i]
                        winner = strategy

                if min_duration >= min_duration_2 * 0.98:
                    winner = "same"
            elif min_duration >= results[mode]["std"][benchmark_name][i] * 0.98:
                winner = "same"

            print(str(i) + ": " + winner)
            winners_per_query[winner] = winners_per_query[winner] + 1
        print(winners_per_query)
