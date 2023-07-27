#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

optimizer_modes = ["dphyp-equisets", "greedy-equisets-ldt", "nostats"]
benchmarks = ["imdb", "ssb"]
adaptive_routing_strategies = ["adaptive_reinit", "dynamic"]
exploration_budgets = ["0.00001", "0.0001", "0.001", "0.01", "0.1", "0.2", "0.4", "0.8", "1.6", "2.4", "3.2"]

results = {}

for mode in optimizer_modes:
    results[mode] = {}
    for benchmark in benchmarks:
        results[mode][benchmark] = {}
        for strategy in adaptive_routing_strategies:
            results[mode][benchmark][strategy] = []
            for budget in exploration_budgets:
                path = os.getcwd() + "/experiment-results/2_1-2_routing_intms/" + mode + "/" + benchmark + "/" \
                       + strategy + "/" + budget

                intms = []
                txt_files = glob.glob(os.path.join(path, "*.txt"))
                if len(txt_files) == 0:
                    print("Warning: " + mode + "/" + benchmark + "/" + strategy + "/" + budget + " missing.")
                    results[mode][benchmark][strategy].append(0)
                    continue
                txt_files.sort()

                for txt_file in txt_files:
                    with open(txt_file) as f:
                        line = f.readline()
                        if line == "":
                            print("Warning: " + txt_file + " empty.")
                            continue
                        intms.append(int(line))

                if len(intms) == 0:
                    print("Warning: " + mode + "/" + benchmark + "/" + strategy + "/" + budget + " has no results.")
                    results[mode][benchmark][strategy].append(0)
                    continue

                results[mode][benchmark][strategy].append(sum(intms))

        strategy = "alternate"
        path = os.getcwd() + "/experiment-results/2_1-2_routing_intms/" + mode + "/" + benchmark + "/" + strategy
        intms_opt = []
        intms_default = []
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        if len(csv_files) == 0:
            print("Warning: " + mode + "/" + benchmark + "/" + strategy + "/" + " missing.")
            results[mode][benchmark]["optimal"] = [0] * len(exploration_budgets)
            results[mode][benchmark]["default"] = [0] * len(exploration_budgets)
            continue
        csv_files.sort()

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            df.pop(df.columns[-1])
            if "path_0" not in df.columns:
                print("Warning: " + csv_file + " corrupted.")
                continue
            intms_opt.append(df.min(axis=1).sum())
            intms_default.append(df["path_0"].sum())

        if len(intms_opt) == 0 or len(intms_default) == 0:
            print("Warning: " + mode + "/" + benchmark + "/" + strategy + "/" + " has no results.")
            results[mode][benchmark]["optimal"] = [0] * len(exploration_budgets)
            results[mode][benchmark]["default"] = [0] * len(exploration_budgets)
            continue

        results[mode][benchmark]["optimal"] = [sum(intms_opt)] * len(exploration_budgets)
        results[mode][benchmark]["default"] = [sum(intms_default)] * len(exploration_budgets)
print(results)

x_values = [float(i) for i in exploration_budgets]

fig = plt.figure(constrained_layout=True)
subfigs = fig.subfigures(nrows=len(optimizer_modes), ncols=1)
for i, subfig in enumerate(subfigs):
    mode = optimizer_modes[i]
    subfig.suptitle(mode)
    axs = subfig.subplots(nrows=1, ncols=len(benchmarks))

    for j, ax in enumerate(axs):
        benchmark = benchmarks[j]
        for strategy in results[mode][benchmark]:
            ax.plot(x_values, results[mode][benchmark][strategy], label=strategy)
        ax.set_title(benchmark)
        ax.set_xscale("log")
        ax.set_ylim(bottom=0)
        ax.set_xticks(x_values)
        if i == len(optimizer_modes) - 1:
            ax.set_xlabel("Exploration budget")
        if j == 0:
            ax.set_ylabel("Intermediates")

fig.legend(labels=results["dphyp-equisets"]["imdb"], loc="outside lower center", ncols=4)
plt.savefig("experiment-results/2_1_routing_expl_intms.pdf")
