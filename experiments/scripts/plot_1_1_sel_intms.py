#!/usr/bin/env python3

import pandas as pd
import os
import glob

results = {}
optimizer_modes = ["dphyp-equisets", "greedy-equisets-ldt"]
benchmarks = ["imdb", "ssb"]
enumerators = ["each_last_once", "each_first_once"]

for optimizer_mode in optimizer_modes:
    for benchmark in benchmarks:
        # Calculate baselines from exhaustive
        path = os.getcwd() + "/experiment-results/1_1_sel_intms/" + optimizer_mode + "/" + benchmark + "/exhaustive"
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

        results[benchmark + "-" + optimizer_mode] = {"default": default, "exhaustive": exhaustive,
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

            results[benchmark + "-" + optimizer_mode][enumerator] = intermediates

        print("### " + benchmark + "-" + optimizer_mode + " ###")
        for mode in results[benchmark + "-" + optimizer_mode]:
            sum_intermediates = sum(results[benchmark + "-" + optimizer_mode][mode])
            print(mode + ": " + str(sum_intermediates / 1000000) + "M intermediates")
