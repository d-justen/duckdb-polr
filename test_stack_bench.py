#!/usr/bin/env python3

import csv
import glob
import multiprocessing as mp
import subprocess as sp
import os

benchmarks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]


def move_files(source, match, target):
    files = glob.glob(os.path.join(source, match))
    if target == "":
        for file in files:
            os.remove(file)
    else:
        for file in files:
            filename = file.split("/")[-1]
            os.rename(file, f"{target}/{filename}")


def move_and_rename(source, match, target):
    files = glob.glob(os.path.join(source, match))
    files.sort()
    for i in range(len(files)):
        os.rename(files[i], f"{target}-{i}.csv")


def execute_benchmark(i, b):
    cwd = os.getcwd()
    sp.call(["mkdir", "-p", f"{cwd}/tmp/{i}"])
    sp.call(["mkdir", "-p", f"{cwd}/experiment-results/stack_test/q{b}"])

    files = glob.glob(os.path.join(f"{cwd}/benchmark/stack/q{b}", "*.benchmark"))
    files.sort()
    query_names = []
    for file in files:
        query_names.append(file.split("/")[-1].split(".")[0])

    for query in query_names:
        sp.call([f"{cwd}/build/release/benchmark/benchmark_runner",
                 f"benchmark/stack/q{b}/{query}.benchmark",
                 "--polr_mode=bushy",
                 "--multiplexer_routing=alternate",
                 "--max_join_orders=24",
                 "--threads=1",
                 "--nruns=1",
                 "--log_tuples_routed",
                 "--disable_caching",
                 f"--dir_prefix={i}"
                 ])

        move_files(f"{cwd}/tmp/{i}", "*.txt", "")
        move_files(f"{cwd}/tmp/{i}", "*-enumeration.csv", "")
        move_and_rename(f"{cwd}/tmp/{i}", "*.csv", f"{cwd}/experiment-results/stack_test/q{b}/{query}")


if __name__ == "__main__":
    sp.call(["rm", "-rf", f"{os.getcwd()}/experiment-results/stack_test"])
    sp.call(["rm", "-rf", f"{os.getcwd()}/tmp"])
    sp.call(["mkdir", "-p", f"{os.getcwd()}/tmp"])
    pool = mp.Pool(mp.cpu_count())

    idx = 0
    for benchmark in benchmarks:
        pool.apply_async(execute_benchmark, args=(idx, benchmark))
        idx += 1

    pool.close()
    pool.join()
