#!/usr/bin/env python3

import pandas as pd
import os
import glob

optimizer_modes = ["dphyp-equisets", "greedy-equisets-ldt", "nostats"]
benchmarks = ["imdb", "ssb"]
enumerators = ["each_last_once", "each_first_once", "dfs_random", "dfs_min_card", "dfs_uncertain", "bfs_random",
               "bfs_min_card", "bfs_uncertain"]

results = {}

for benchmark in benchmarks:
    for optimizer_mode in optimizer_modes:
        # Calculate baselines from exhaustive
        path = os.getcwd() + "/experiment-results/1_1_sel_intms/" + optimizer_mode + "/" + benchmark + "/bfs_min_card"
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        csv_files.sort()

        default = []
        exhaustive = []
        best_in_class = []
        worst_in_class = []

        for csv_file in csv_files:
            df = pd.read_csv(csv_file);
            df.pop(df.columns[-1])

            default.append(df["path_0"].sum())
            exhaustive.append(df.min(axis=1).sum())
            best_in_class.append(df.sum().min())
            worst_in_class.append(df.sum().max())

        results[benchmark + "-" + optimizer_mode[:2]] = {"default": default, "exhaustive": exhaustive,
                                                     "best_in_class": best_in_class, "worst_in_class": worst_in_class}

        for enumerator in enumerators:
            path = os.getcwd() + "/experiment-results/1_1_sel_intms/" + optimizer_mode + "/" + benchmark + "/" + enumerator
            csv_files = glob.glob(os.path.join(path, "*.csv"))
            csv_files.sort()

            intermediates = []

            for csv_file in csv_files:
                df = pd.read_csv(csv_file)
                df.pop(df.columns[-1])
                intermediates.append(df.min(axis=1).sum())

            results[benchmark + "-" + optimizer_mode[:2]][enumerator] = intermediates

        print("### " + benchmark + "-" + optimizer_mode + " ###")
        for mode in results[benchmark + "-" + optimizer_mode[:2]]:
            sum_intermediates = sum(results[benchmark + "-" + optimizer_mode[:2]][mode])
            print(mode + ": " + str(sum_intermediates / 1000000) + "M intermediates")


result_str = "\\begin{table}\n\t\\centering\n\t\\begin{tabular}{l"

for i in range(len(results)):
    result_str += "r"

result_str += "}\n\t\t"
result_str += "\\textbf{Join order selection}"

for result_key in results:
    result_str += " & " + result_key

result_str += "\\\\\n\t\t"
result_str += "\\hline\n\t\t"

enumerators = list(results[list(results.keys())[0]].keys())

for enumerator in enumerators:
    result_str += enumerator.replace("_", " ")
    for result_key in results:
        sum_intermediates = sum(results[result_key][enumerator])
        result_str += " & " + "{:10.2f}".format(sum_intermediates / 1000000) + " M"

    result_str += "\\\\\n\t\t"

result_str += "\hline\n\t\\end{tabular}\n\t\\caption{"
result_str += "Total number of intermediates for POLAR pipelines with optimal routing strategies"
result_str += "}\n\t\\label{"
result_str += "tab:1_1_sel_intms"
result_str += "}\n\\end{table}\n"

with open("experiment-results/1_1_sel_intms.txt", "w") as file:
    file.write(result_str)