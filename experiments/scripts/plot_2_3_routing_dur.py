#!/usr/bin/env python3

import pandas as pd
import os
import glob

routing_strategies = ["default", "init_once", "opportunistic", "adaptive_reinit", "dynamic"]
optimizer_modes = ["dphyp-equisets", "greedy-equisets-ldt", "nostats"]
benchmarks = ["imdb", "ssb"]
raw_results = {}

for benchmark in benchmarks:
    for mode in optimizer_modes:
        raw_results[f"{benchmark}-{mode[:2]}"] = {}
        for strategy in routing_strategies:
            path = os.getcwd() + f"/experiment-results/2_3_routing_dur/{mode}/{benchmark}/{strategy}"
            csv_files = glob.glob(os.path.join(path, "*.csv"))
            if len(csv_files) == 0:
                print(f"Warning: no results for {path}")
                continue

            timings = []
            for csv_file in csv_files:
                df = pd.read_csv(csv_file, names=["timing"])
                avg_timing = float(df["timing"].median())
                timings.append(avg_timing)

            raw_results[f"{benchmark}-{mode[:2]}"][strategy] = timings

result_str = "\\begin{table}\n\t\\centering\n\t\\begin{tabular}{l"

for i in range(len(raw_results)):
    result_str += "r"

result_str += "}\n\t\t"
result_str += "\\textbf{Routing strategy}"

for result_key in raw_results:
    result_str += " & " + result_key

result_str += "\\\\\n\t\t"
result_str += "\\hline\n\t\t"

for routing_strategy in routing_strategies:
    result_str += routing_strategy.replace("_", " ")

    for result_key in raw_results:
        if len(raw_results[result_key]["default"]) > 0:
            total_pipeline_duration = sum(raw_results[result_key][routing_strategy])
            result_str += " & " + "{:10.2f}".format(total_pipeline_duration / 1000) + " s"
    result_str += "\\\\\n\t\t"

result_str += "\hline\n\t\\end{tabular}\n\t\\caption{"
result_str += "Total duration for POLAR-applicable pipelines"
result_str += "}\n\t\\label{"
result_str += "tab:2_3_routing_dur"
result_str += "}\n\\end{table}\n"

with open("experiment-results/2_3_routing_dur.txt", "w") as file:
    file.write(result_str)
