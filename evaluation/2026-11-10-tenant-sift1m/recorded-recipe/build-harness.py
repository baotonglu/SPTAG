#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=["legacy", "tenant"])
parser.add_argument("--name")
args = parser.parse_args()
root = Path(__file__).resolve().parent
repo = Path("/home/baotonglu/SPTAG")
source = root / "baseline-source" if args.mode == "legacy" else repo
name = args.name or ("baseline" if args.mode == "legacy" else "current")
command = [
    "g++", "-std=c++20", "-O3", "-DNDEBUG", "-fopenmp", "-fno-omit-frame-pointer",
    "-DTBB", "-DROCKSDB", "-DNUMA",
    *(["-DTENANT_BENCH_CURRENT"] if args.mode == "tenant" else []),
    "-I", str(source / "AnnService"), str(repo / "Test/tenant_perf.cpp"),
    "-o", str(root / "bin" / f"{name}-tenantperf"), "-L", str(source / "Release"),
    "-lSPTAGLibStatic", "-lDistanceUtils", "-lrocksdb", "-lzstd",
    "-lboost_system", "-lboost_thread", "-lboost_serialization", "-lboost_wserialization",
    "-lboost_regex", "-lboost_filesystem", "-ltbb", "-lnuma", "-ldl", "-pthread", "-lrt", "-lm",
]
(root / f"{name}-compile-command.json").write_text(json.dumps(command, indent=2) + "\n")
with (root / f"{name}-final-harness-build.log").open("w") as log:
    subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
