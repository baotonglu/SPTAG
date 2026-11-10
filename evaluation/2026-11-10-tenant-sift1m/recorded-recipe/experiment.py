#!/usr/bin/env python3
"""Prepare, run and verify the matched before/after SIFT1M experiment."""

import argparse
import csv
import json
import os
from pathlib import Path
import statistics
import struct
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REPO = Path("/home/baotonglu/SPTAG")
sys.path.insert(0, str(REPO / "Test"))
import sift_benchmark as sift


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def prepare():
    import numpy as np

    data = ROOT / "input"
    base = data / "sift-first-1m.u8bin"
    if not base.exists():
        source = data / "base-first-1m.download"
        assert source.stat().st_size == 128000008
        with source.open("rb") as stream, base.open("xb") as target:
            assert struct.unpack("<ii", stream.read(8)) == (1000000000, 128)
            target.write(struct.pack("<ii", 1000000, 128))
            while block := stream.read(8 << 20):
                target.write(block)
    queries_path = data / "queries-200.u8bin"
    if not queries_path.exists():
        sift.write_vectors(queries_path, sift.read_vectors(np, data / "query.public.10K.u8bin")[:200])
    vectors = sift.read_vectors(np, base)
    queries = sift.read_vectors(np, queries_path)
    paths = [base, data / "query.public.10K.u8bin", queries_path]
    save(data / "checksums.json", {p.name: sift.checksum(p) for p in paths})
    for phase in range(11):
        destination = data / f"truth-{phase}.npz"
        if destination.exists():
            continue
        count = 990000 + phase * 1000
        ids, distances = sift.truth(np, vectors[:count], queries)
        np.savez(destination, ids=ids, distances=distances, count=count)
        print(f"truth phase {phase} complete", flush=True)


def run(compaction_control=False):
    cpus = sorted(os.sched_getaffinity(0))[:16]
    if len(cpus) != 16:
        raise RuntimeError("The fixed SIFT1M experiment requires 16 available CPUs")
    os.sched_setaffinity(0, cpus)
    env = dict(os.environ, OMP_NUM_THREADS="8", OPENBLAS_NUM_THREADS="1",
               TENANT_PERF_CONFIG=str(REPO / "Test/tenant_sift1m.ini"))
    binaries = {"legacy": ROOT / "bin/baseline-tenantperf", "tenant": ROOT / "bin/current-tenantperf"}
    if compaction_control:
        binaries = {"legacy-no-compaction": ROOT / "bin/baseline-no-compaction-tenantperf"}
        env["TENANT_PERF_FORCE_COMPACTION"] = "0"
    for binary in binaries.values():
        if not binary.is_file():
            raise RuntimeError(f"Missing executable: {binary}")
    manifest_path = ROOT / ("runs-no-compaction.json" if compaction_control else "runs.json")
    if manifest_path.exists():
        raise RuntimeError("Refusing to overwrite an experiment")
    manifest = {
        "cpu_affinity": cpus, "environment_overrides": {k: env[k] for k in (
            "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "TENANT_PERF_CONFIG")},
        "force_native_compaction": not compaction_control,
        "binary_sha256": {k: sift.checksum(p) for k, p in binaries.items()},
        "profile_sha256": sift.checksum(REPO / "Test/tenant_sift1m.ini"),
        "method": ("Three additional serial native builds with forced compaction disabled; "
                   if compaction_control else "Three independent builds per mode, serial AB/BA/AB order; ") +
                  "990K + 10x1K, four writers, no concurrent readers; first 200 queries, "
                  "16 query threads, warmup before each of two measured passes per phase; "
                  "insert timer includes any enabled compaction and all derived-work drain; "
                  "save/checkpoint timed separately. No cache flush, fixed build seed or host isolation.",
        "runs": [],
    }
    save(manifest_path, manifest)
    for repeat in range(1, 4):
        order = ("legacy-no-compaction",) if compaction_control else (
            ("legacy", "tenant") if repeat % 2 else ("tenant", "legacy"))
        for mode in order:
            name = f"{mode}-{repeat}"
            directory = ROOT / name
            directory.mkdir()
            command = [
                "timeout", "--signal=TERM", "--kill-after=30s", "7200s",
                "nice", "-n", "10", "/usr/bin/time", "-v", "-o", str(directory / "resources.txt"),
                str(binaries[mode]), "updates", str(directory / "node"),
                str(ROOT / "input/sift-first-1m.u8bin"), str(ROOT / "input/queries-200.u8bin"),
                str(directory / "results"), "990000", "10000", "1000", "4", "0",
                "legacy" if mode == "legacy-no-compaction" else mode,
            ]
            record = {"name": name, "mode": mode, "repeat": repeat, "command": command,
                      "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            manifest["runs"].append(record)
            save(manifest_path, manifest)
            print(f"starting {name}", flush=True)
            start = time.monotonic()
            with (directory / "run.log").open("x") as log:
                result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            record["exit_code"] = result.returncode
            record["wall_s"] = time.monotonic() - start
            save(manifest_path, manifest)
            if record["exit_code"]:
                raise RuntimeError(f"{name} failed; see {directory / 'run.log'}")
            print(f"completed {name} in {record['wall_s']:.1f}s", flush=True)


def analyze():
    import numpy as np

    manifest = json.loads((ROOT / "runs.json").read_text())
    controls = ROOT / "runs-no-compaction.json"
    if controls.exists():
        manifest["runs"].extend(json.loads(controls.read_text())["runs"])
    base = sift.read_vectors(np, ROOT / "input/sift-first-1m.u8bin")
    queries = sift.read_vectors(np, ROOT / "input/queries-200.u8bin")
    reports = []
    for run in manifest["runs"]:
        if run.get("exit_code") != 0:
            raise RuntimeError(f"Incomplete run {run['name']}")
        directory = ROOT / run["name"] / "results"
        with (directory / "id-map.csv").open() as stream:
            mapping = list(csv.DictReader(stream))
        assert len(mapping) == 10000
        assert {int(r["input_id"]) for r in mapping} == set(range(990000, 1000000))
        assert {int(r["assigned_id"]) for r in mapping} == set(range(990000, 1000000))
        with (directory / "updates.csv").open() as stream:
            updates = list(csv.DictReader(stream))
        assert len(updates) == 10
        assert [int(r["phase"]) for r in updates] == list(range(1, 11))
        assert [int(r["count"]) for r in updates] == list(range(991000, 1000001, 1000))
        assert all(int(r["query_errors"]) == 0 for r in updates)
        with (directory / "query-batches.csv").open() as stream:
            batches = list(csv.DictReader(stream))
        assert len(batches) == 24
        assert all(int(r["queries"]) == 200 and int(r["threads"]) == 16 for r in batches)
        assert {(int(r["phase"]), int(r["round"])) for r in batches} == {
            (p, r) for p in range(12) for r in (1, 2)}
        recall = []
        final_bytes = None
        for batch in batches:
            phase, round_id = int(batch["phase"]), int(batch["round"])
            expected = np.load(ROOT / f"input/truth-{min(phase, 10)}.npz")
            truth_ids, truth_dist = expected["ids"], expected["distances"]
            path = directory / f"phase-{phase}-round-{round_id}.csv"
            with path.open() as stream:
                rows = list(csv.DictReader(stream))
            assert len(rows) == 1000
            expected_order = [(q, rank) for q in range(200) for rank in range(5)]
            assert [(int(r["query"]), int(r["rank"])) for r in rows] == expected_order
            strict = tolerant = 0
            count = 990000 + min(phase, 10) * 1000
            for q in range(200):
                found = rows[q * 5:(q + 1) * 5]
                ids = [int(r["id"]) for r in found]
                assert len(set(ids)) == 5 and all(0 <= i < count for i in ids)
                actual = np.sum((base[ids].astype(np.int64) - queries[q].astype(np.int64)) ** 2, axis=1)
                assert actual.tolist() == [float(r["distance"]) for r in found]
                strict += len(set(ids) & set(truth_ids[q].tolist()))
                unused = set(range(5))
                for tid, distance in zip(truth_ids[q], truth_dist[q]):
                    for rank in sorted(unused):
                        if ids[rank] == tid or abs(actual[rank] - distance) < 1e-4 * (abs(distance) + 1e-4):
                            unused.remove(rank)
                            tolerant += 1
                            break
            recall.append({"phase": phase, "round": round_id, "strict": strict / 1000,
                           "tolerant": tolerant / 1000})
            if phase == 10 and round_id == 2:
                final_bytes = path.read_bytes()
            if phase == 11 and round_id == 2:
                assert path.read_bytes() == final_bytes, "Final and reopened results differ"
        final = next(r for r in batches if int(r["phase"]) == 10 and int(r["round"]) == 2)
        insert_s = sum(float(r["insert_s"]) for r in updates)
        with (directory / "build.csv").open() as stream:
            build = list(csv.DictReader(stream))
        assert len(build) == 1 and int(build[0]["count"]) == 990000
        report = {
            "name": run["name"], "mode": run["mode"], "final_round2_qps": float(final["qps"]),
            "final_round2_p99_ms": float(final["p99_ms"]),
            "build_and_initial_save_s": float(build[0]["build_and_initial_save_s"]),
            "insertion_aggregate_vectors_per_s": 10000 / insert_s,
            "insertion_total_s": insert_s,
            "compaction_total_s": sum(float(r["compaction_s"]) for r in updates),
            "drain_total_s": sum(float(r["drain_s"]) for r in updates),
            "save_checkpoint_total_s": sum(float(r["checkpoint_s"]) for r in updates),
            "final_round2_recall": next(r for r in recall if r["phase"] == 10 and r["round"] == 2),
            "recall_by_phase": recall, "query_batches": batches, "updates": updates,
            "ack_vid_bijection": True, "exact_returned_distances": True, "reopen_identical": True,
        }
        reports.append(report)
        save(ROOT / run["name"] / "verified.json", report)
    summary = {}
    modes = ["legacy", "tenant"]
    if controls.exists():
        modes.append("legacy-no-compaction")
    for mode in modes:
        selected = [r for r in reports if r["mode"] == mode]
        assert len(selected) == 3
        metrics = {}
        for key in ("final_round2_qps", "final_round2_p99_ms", "build_and_initial_save_s",
                    "insertion_aggregate_vectors_per_s",
                    "save_checkpoint_total_s"):
            values = [r[key] for r in selected]
            metrics[key] = {"values": values, "median": statistics.median(values),
                            "min": min(values), "max": max(values)}
        metrics["final_recall"] = [r["final_round2_recall"] for r in selected]
        summary[mode] = metrics
    changes = {key: 100 * (summary["tenant"][key]["median"] / summary["legacy"][key]["median"] - 1)
               for key in ("final_round2_qps", "final_round2_p99_ms", "insertion_aggregate_vectors_per_s")}
    controlled_changes = None
    if controls.exists():
        controlled_changes = {key: 100 * (summary["tenant"][key]["median"] /
                                         summary["legacy-no-compaction"][key]["median"] - 1)
                              for key in changes}
    save(ROOT / "summary.json", {"modes": summary, "tenant_change_percent": changes,
                                 "tenant_vs_no_compaction_percent": controlled_changes, "runs": reports})
    print(json.dumps({"modes": summary, "tenant_change_percent": changes,
                      "tenant_vs_no_compaction_percent": controlled_changes}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "run", "control", "analyze"])
    args = parser.parse_args()
    if args.command == "control":
        run(compaction_control=True)
    else:
        globals()[args.command]()
