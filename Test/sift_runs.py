#!/usr/bin/env python3
"""Run ordered, isolated SIFT comparisons with tenantperf binaries."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", required=True, type=Path)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--stage", choices=["single", "cache", "updates", "profile"], default="single")
    parser.add_argument("--after", type=Path, help="Wait for this successful manifest before using CPUs")
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.after:
        deadline = time.monotonic() + 14400
        while True:
            if args.after.exists():
                previous = json.loads(args.after.read_text())
                if any(r.get("exit_code", 0) != 0 for r in previous["runs"]):
                    raise RuntimeError("Prerequisite experiment failed")
                if previous.get("completed"):
                    break
            if time.monotonic() > deadline:
                raise TimeoutError("Prerequisite did not complete")
            time.sleep(10)
    if args.output.exists():
        raise ValueError("Choose a fresh output directory")
    args.output.mkdir(parents=True)
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])
    env = dict(os.environ, OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1")
    preload = subprocess.check_output(["g++", "-print-file-name=libstdc++.so.6"], text=True).strip()
    env["LD_PRELOAD"] = preload
    queries = args.data / "query.public.10K.u8bin"
    manifest = {
        "stage": args.stage, "seconds": args.seconds, "repeats": args.repeats,
        "cpus": sorted(os.sched_getaffinity(0)),
        "original_binary_sha256": digest(args.original),
        "current_binary_sha256": digest(args.current),
        "queries_sha256": digest(queries),
        "dataset": json.loads((args.data / "dataset.json").read_text()),
        "runner_sha256": digest(Path(__file__)),
        "runs": [],
    }

    def save():
        tmp = args.output / "manifest.tmp"
        tmp.write_text(json.dumps(manifest, indent=2) + "\n")
        tmp.replace(args.output / "manifest.json")

    def run(name, binary, command, timeout=7200):
        argv = [str(binary.resolve()), *map(str, command)]
        item = {"name": name, "argv": argv, "started_unix": time.time()}
        manifest["runs"].append(item)
        save()
        print(f"START {name}", flush=True)
        with (args.output / f"{name}.log").open("x") as log:
            result = subprocess.run(argv, env=env, stdout=log, stderr=subprocess.STDOUT,
                                    timeout=timeout)
        item.update(exit_code=result.returncode, wall_seconds=time.time() - item["started_unix"])
        save()
        if result.returncode:
            raise RuntimeError(f"{name} failed: see its log")
        print(f"DONE {name}: {item['wall_seconds']:.2f}s", flush=True)

    def measure(name, binary, mode, root, slots, concurrency, mix, names):
        output = args.output / f"{name}.csv"
        run(name, binary, ["query", mode, root, queries, output, slots,
                          concurrency, args.seconds, 5, mix, *names])
        lines = output.read_text().split("# measured\n")
        if len(lines) != 2:
            raise RuntimeError("Missing measurement section")
        rows = list(csv.reader(lines[1].splitlines()))
        total = next(row for row in rows if row[:2] == ["query", "all"])
        if int(total[5]) or int(total[4]) == 0:
            raise RuntimeError(f"{name}: errors or no completions")
        manifest["runs"][-1]["measurement"] = {
            "qps": float(total[6]), "p50_ms": float(total[7]), "p95_ms": float(total[8]),
            "p99_ms": float(total[9]), "cpu_cores": float(total[11]),
            "process_peak_rss_kib": int(total[12]), "errors": int(total[5]),
        }
        save()

    save()
    if args.stage == "single":
        legacy = args.output / "legacy-index"
        tenant = args.output / "tenant-node"
        base = args.data / "sift-first-1m.u8bin"
        run("build-original", args.original, ["build", "legacy", legacy, base, 1000000, "only"])
        run("build-tenant", args.current, ["build", "tenant", tenant, base, 1000000, "only"])
        versions = [
            ("A-original", args.original, "legacy", legacy),
            ("B-current-legacy", args.current, "legacy", legacy),
            ("C-current-tenant", args.current, "tenant", tenant),
        ]
        for label, binary, mode, root in versions:
            run(f"accuracy-{label}", binary, [
                "accuracy", mode, root, queries, args.output / f"accuracy-{label}.csv", 8, "only"])
        for repeat in range(args.repeats):
            # Rotate version order, reverse concurrency order every other repeat.
            order = versions[repeat % 3:] + versions[:repeat % 3]
            for concurrency in ([1, 4, 8, 16] if repeat % 2 == 0 else [16, 8, 4, 1]):
                for label, binary, mode, root in order:
                    measure(f"{label}-c{concurrency}-r{repeat}", binary, mode, root, 8,
                            concurrency, "uniform", ["only"])
    elif args.stage == "cache":
        root = args.output / "tenant-node"
        names = [f"tenant-{i}" for i in range(4)]
        for i, count in enumerate([100000, 200000, 300000, 400000]):
            run(f"build-{i}", args.current, [
                "build", "tenant", root, args.data / f"tenant-{i}.u8bin", count, names[i]])
        for name in names:
            run(f"accuracy-{name}", args.current, [
                "accuracy", "tenant", root, queries, args.output / f"accuracy-{name}.csv", 8, name])
            truth_path = args.output / f"truth-{name}.npz"
            run(f"truth-{name}", Path(sys.executable), [
                Path(__file__).with_name("sift_benchmark.py").resolve(), "truth",
                args.data / f"{name}.u8bin", queries, truth_path])
            run(f"recall-{name}", Path(sys.executable), [
                Path(__file__).with_name("sift_benchmark.py").resolve(), "evaluate",
                args.output / f"accuracy-{name}.csv", truth_path, args.output / f"recall-{name}.json"])
        for repeat in range(args.repeats):
            for concurrency in ([1, 4, 8, 16] if repeat % 2 == 0 else [16, 8, 4, 1]):
                for mix in ["uniform", "hot"]:
                    for slots in ([8, 2] if repeat % 2 == 0 else [2, 8]):
                        measure(f"slots{slots}-{mix}-c{concurrency}-r{repeat}", args.current,
                                "tenant", root, slots, concurrency, mix, names)
    elif args.stage == "updates":
        queries = args.data / "queries-200.u8bin"
        base = args.data / "sift-first-1m.u8bin"
        for readers in [0, 4]:
            label = f"readers{readers}"
            output = args.output / label
            run(label, args.current, [
                "updates", args.output / f"node-{label}", base, queries, output,
                990000, 10000, 1000, 4, readers])
        for phase in range(11):
            truth_path = args.output / f"truth-{phase}.npz"
            run(f"truth-{phase}", Path(sys.executable), [
                Path(__file__).with_name("sift_benchmark.py").resolve(), "truth",
                base, queries, truth_path, "--count", 990000 + phase * 1000])
            for readers in [0, 4]:
                output = args.output / f"readers{readers}"
                run(f"recall-r{readers}-p{phase}", Path(sys.executable), [
                    Path(__file__).with_name("sift_benchmark.py").resolve(), "evaluate",
                    output / f"phase-{phase}.csv", truth_path, output / f"recall-{phase}.json"])
        for readers in [0, 4]:
            output = args.output / f"readers{readers}"
            if (output / "phase-10.csv").read_bytes() != (output / "phase-11.csv").read_bytes():
                raise RuntimeError("Queries differ after checkpoint/reopen")
    else:
        if not args.after:
            raise ValueError("Profile stage requires the single-stage manifest via --after")
        previous_root = args.after.parent
        for label, binary, mode, root in [
            ("A-original", args.original, "legacy", previous_root / "legacy-index"),
            ("B-current-legacy", args.current, "legacy", previous_root / "legacy-index"),
            ("C-current-tenant", args.current, "tenant", previous_root / "tenant-node"),
        ]:
            run(f"recall-{label}", Path(sys.executable), [
                Path(__file__).with_name("sift_benchmark.py").resolve(), "evaluate",
                previous_root / f"accuracy-{label}.csv", args.data / "truth-1m.npz",
                args.output / f"recall-{label}.json"])
            run(f"profile-{label}", Path(sys.executable), [
                Path(__file__).with_name("sift_profile.py").resolve(),
                "--output", args.output / f"stacks-{label}.log", "--samples", 60, "--",
                binary, "query", mode, root, queries, args.output / f"diagnostic-{label}.csv",
                8, 8, 60, 5, "uniform", "only"])
    manifest["completed"] = True
    save()


if __name__ == "__main__":
    main()
