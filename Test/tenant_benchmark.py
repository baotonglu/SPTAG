#!/usr/bin/env python3
"""Reproducible, warm-resident four-tenant TCP serving benchmark (Linux)."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import socket
import struct
import subprocess
import sys
import threading
import time


def save_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def process_usage(pid):
    # Fields after the final ')' begin with field 3 (state).
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return {
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
        "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
    }


def exact_neighbors(np, base, queries, k):
    """Blockwise float64 squared-L2 ground truth, without an N x Q allocation."""
    truth = []
    for start in range(0, len(queries), 32):
        query = queries[start:start + 32].astype(np.float64)
        best_dist = np.full((len(query), k), np.inf)
        best_ids = np.full((len(query), k), -1, dtype=np.int32)
        for offset in range(0, len(base), 8192):
            block = base[offset:offset + 8192].astype(np.float64)
            distance = (np.sum(query * query, axis=1)[:, None]
                        + np.sum(block * block, axis=1)[None, :]
                        - 2 * query @ block.T)
            ids = np.broadcast_to(np.arange(offset, offset + len(block)), distance.shape)
            distance = np.concatenate((best_dist, distance), axis=1)
            ids = np.concatenate((best_ids, ids), axis=1)
            selected = np.argpartition(distance, k - 1, axis=1)[:, :k]
            best_dist = np.take_along_axis(distance, selected, axis=1)
            best_ids = np.take_along_axis(ids, selected, axis=1)
        truth.extend(best_ids.tolist())
    return np.asarray(truth, dtype=np.int32)


def prepare(np, args, binaries, output, env):
    settings = {
        "sizes": args.sizes, "dimension": args.dimension, "queries": args.queries,
        "k": args.k, "seed": args.seed, "distribution": "independent standard normal",
        "dtype": "Float/float32", "metric": "L2",
    }
    manifest_path = output / "dataset.json"
    if args.reuse:
        manifest = json.loads(manifest_path.read_text())
        if manifest["settings"] != settings or len(manifest["tenants"]) != 4:
            raise ValueError("--reuse requires a completed dataset with matching settings")
    else:
        if output.exists():
            raise ValueError("Output already exists; choose a fresh directory or --reuse")
        output.mkdir(parents=True)
        manifest = {"settings": settings, "tenants": []}
        for tenant_id, size in enumerate(args.sizes):
            name = f"tenant-{tenant_id}-{size}"
            rng = np.random.default_rng(args.seed + tenant_id)
            base = rng.standard_normal((size, args.dimension), dtype=np.float32)
            queries = rng.standard_normal((args.queries, args.dimension), dtype=np.float32)
            print(f"Preparing {name}: exact ground truth and index", flush=True)
            truth = exact_neighbors(np, base, queries, args.k)
            np.save(output / f"{name}-queries.npy", queries)
            np.save(output / f"{name}-truth.npy", truth)
            vector_path = output / f"{name}.bin"
            with vector_path.open("wb") as stream:
                stream.write(struct.pack("<ii", size, args.dimension))
                base.tofile(stream)
            del base
            start = time.perf_counter()
            with (output / f"{name}-build.log").open("w") as log:
                subprocess.run(
                    [str(binaries / "tenanttool"), str(output / "node"), "create",
                     name, "Float", "L2", str(vector_path)],
                    stdout=log, stderr=subprocess.STDOUT, env=env, check=True, timeout=3600,
                )
            manifest["tenants"].append({
                "name": name, "size": size, "build_wall_seconds": time.perf_counter() - start,
            })
            save_json(manifest_path, manifest)
            print(f"Built {name} in {manifest['tenants'][-1]['build_wall_seconds']:.1f}s", flush=True)
    queries = []
    truth = []
    for tenant in manifest["tenants"]:
        values = np.load(output / f"{tenant['name']}-queries.npy")
        queries.append([row.tobytes() for row in values])
        truth.append([set(row.tolist()) for row in np.load(output / f"{tenant['name']}-truth.npy")])
    return manifest, queries, truth


def connected_client(client_module, port, server, timeout):
    client = client_module.AnnClient("127.0.0.1", str(port))
    deadline = time.monotonic() + timeout
    while not client.IsConnected():
        if server.poll() is not None:
            raise RuntimeError("Server exited; inspect server.log")
        if time.monotonic() >= deadline:
            raise TimeoutError("Client connection timed out")
        time.sleep(0.05)
    client.SetTimeoutMilliseconds(30000)
    return client


def measurement(np, clients, server, tenants, queries, k, concurrency, seconds, seed):
    if server.poll() is not None:
        raise RuntimeError("Server exited before measurement")
    start_gate = threading.Barrier(concurrency + 1)
    stop_at = [0.0]

    def worker(worker_id):
        # All workers rotate tenants, so C=1 also serves all four tenants.
        rng = random.Random(seed + worker_id)
        records = [[] for _ in tenants]
        request = worker_id
        start_gate.wait()
        while time.perf_counter() < stop_at[0]:
            tenant = request % len(tenants)
            query_id = rng.randrange(len(queries[tenant]))
            before = time.perf_counter()
            result = clients[worker_id].SearchTenant(
                tenants[tenant]["name"], queries[tenant][query_id], k, "Float")
            after = time.perf_counter()
            records[tenant].append((after, (after - before) * 1000, result["status_name"]))
            request += 1
        return records

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, worker_id) for worker_id in range(concurrency)]
        server_before = process_usage(server.pid)
        client_before = process_usage(os.getpid())
        start = time.perf_counter()
        stop_at[0] = start + seconds
        start_gate.wait()
        records = [future.result() for future in futures]
        elapsed = time.perf_counter() - start
        server_after = process_usage(server.pid)
        client_after = process_usage(os.getpid())
    per_tenant = []
    all_latency = []
    all_errors = Counter()
    completed_in_window = 0
    for tenant_id, tenant in enumerate(tenants):
        rows = [row for worker_rows in records for row in worker_rows[tenant_id]]
        latency = [row[1] for row in rows if row[2] == "Success"]
        errors = Counter(row[2] for row in rows if row[2] != "Success")
        in_window = sum(row[0] <= stop_at[0] and row[2] == "Success" for row in rows)
        completed_in_window += in_window
        all_latency.extend(latency)
        all_errors.update(errors)
        per_tenant.append({
            "tenant": tenant["name"], "requests": len(rows), "successful": len(latency),
            "successful_qps": in_window / seconds,
            "error_rate": sum(errors.values()) / max(1, len(rows)), "errors": dict(errors),
            "latency_ms_p50_p95_p99": np.percentile(latency, [50, 95, 99]).tolist() if latency else None,
        })
    return {
        "concurrency": concurrency, "measurement_seconds": seconds, "elapsed_with_drain_seconds": elapsed,
        "successful_qps": completed_in_window / seconds,
        "successful": len(all_latency), "errors": dict(all_errors),
        "error_rate": sum(all_errors.values()) / max(1, len(all_latency) + sum(all_errors.values())),
        "latency_ms_p50_p95_p99": np.percentile(all_latency, [50, 95, 99]).tolist() if all_latency else None,
        "server_cpu_cores_used": (server_after["cpu_seconds"] - server_before["cpu_seconds"]) / elapsed,
        "client_cpu_cores_used": (client_after["cpu_seconds"] - client_before["cpu_seconds"]) / elapsed,
        "server_rss_bytes": server_after["rss_bytes"], "tenants": per_tenant,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("Release"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sizes", type=int, nargs=4, default=[10000, 50000, 100000, 200000])
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--queries", type=int, default=1024)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 8, 16])
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cpus", type=int, default=8)
    parser.add_argument("--reuse", action="store_true")
    parser.add_argument("--run-name", default="results")
    parser.add_argument("--resident-tenants", type=int, default=8)
    parser.add_argument("--resident-bytes", type=int, default=0)
    args = parser.parse_args()
    if (min(args.sizes) < 64 or max(args.sizes) > 1000000 or not 1 <= args.dimension <= 4096
            or not 1 <= args.k <= min(1000, min(args.sizes)) or args.queries < 1
            or min(args.concurrency) < 1 or args.seconds <= 0 or args.warmup <= 0
            or args.repeats < 1 or not 1 <= args.cpus <= 8
            or args.resident_tenants < 1 or args.resident_bytes < 0
            or not args.run_name or Path(args.run_name).name != args.run_name
            or args.run_name in (".", "..")):
        parser.error("Invalid sizes, shape, timing, concurrency or CPU count")
    affinity = sorted(os.sched_getaffinity(0))[:args.cpus]
    os.sched_setaffinity(0, affinity)
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    import numpy as np
    binaries = args.bin_dir.resolve()
    output = args.output.resolve()
    sys.path.insert(0, str(binaries))
    import SPTAGClient
    env = dict(os.environ)
    manifest, queries, truth = prepare(np, args, binaries, output, env)
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    config = output / "server.ini"
    config.write_text(
        f"[Service]\nListenAddr=127.0.0.1\nListenPort={port}\nThreadNumber={len(affinity)}\n"
        f"SocketThreadNumber=1\n[MultiTenant]\nRoot={output / 'node'}\n"
        f"MaxResidentTenants={args.resident_tenants}\nMaxResidentBytes={args.resident_bytes}\n"
    )
    report = {
        "dataset": manifest, "platform": platform.platform(), "python": sys.version,
        "numpy": np.__version__, "cpu_affinity": affinity, "server_workers": len(affinity),
        "server_sha256": hashlib.sha256((binaries / "server").read_bytes()).hexdigest(),
        "client_sha256": hashlib.sha256((binaries / "_SPTAGClient.so").read_bytes()).hexdigest(),
        "concurrency": args.concurrency, "seconds": args.seconds, "warmup": args.warmup,
        "repeats": args.repeats, "recall": [], "runs": [],
        "resident_tenant_limit": args.resident_tenants, "resident_byte_target": args.resident_bytes,
        "search_parameters": {"head_ratio": 0.2, "internal_results": 32, "max_check": 256, "replica_count": 2},
        "shutdown": "SIGKILL after read-only measurements; offline creation already checkpointed the dataset. "
                    "Avoids the separately documented legacy socket shutdown endpoint exception.",
        "method": "Loopback TCP, one client/connection per worker, synchronous closed-loop; "
                  "equal request counts per tenant, random queries with replacement; "
                  "residency controlled by configured limits, no writes, no timed checkpoint; client and server "
                  "share the listed CPUs. QPS counts successful completions within the timed window; "
                  "latency includes drain requests, excludes errors, and is not coordinated-omission corrected.",
    }
    result_path = output / f"{args.run_name}.json"
    save_json(result_path, report)
    server = None
    clients = []
    stopped_after_measurement = False
    with (output / f"{args.run_name}-server.log").open("w") as log:
        try:
            server = subprocess.Popen(
                [str(binaries / "server"), "-m", "socket", "-c", str(config)],
                env=env, stdout=log, stderr=subprocess.STDOUT,
            )
            # Use a real persistent client for readiness, not a connect-and-immediately-close probe.
            clients = [connected_client(SPTAGClient, port, server, 300)
                       for _ in range(max(args.concurrency))]
            for tenant_id, tenant in enumerate(manifest["tenants"]):
                recalls = []
                for query_id, query in enumerate(queries[tenant_id]):
                    result = clients[0].SearchTenant(tenant["name"], query, args.k, "Float")
                    if result["status"] != 0:
                        raise RuntimeError(f"Recall query failed: {result}")
                    recalls.append(len(set(result["ids"]) & truth[tenant_id][query_id]) / args.k)
                report["recall"].append({"tenant": tenant["name"], "recall_at_k": float(np.mean(recalls))})
                print(f"{tenant['name']}: recall@{args.k}={np.mean(recalls):.4f}", flush=True)
            save_json(result_path, report)
            for repeat in range(args.repeats):
                # Alternate order to reduce systematic warm-cache/order bias.
                order = args.concurrency if repeat % 2 == 0 else list(reversed(args.concurrency))
                for concurrency in order:
                    measurement(np, clients, server, manifest["tenants"],
                                queries, args.k, concurrency, args.warmup, args.seed + repeat)
                    result = measurement(np, clients, server, manifest["tenants"],
                                         queries, args.k, concurrency, args.seconds, args.seed + repeat + 100)
                    result["repeat"] = repeat + 1
                    report["runs"].append(result)
                    save_json(result_path, report)
                    print(f"repeat={repeat + 1} concurrency={concurrency}: "
                          f"QPS={result['successful_qps']:.1f}, "
                          f"p50/p95/p99(ms)={result['latency_ms_p50_p95_p99']}, "
                          f"errors={result['errors']}", flush=True)
            if server.poll() is not None:
                raise RuntimeError("Server exited during measurement")
            stopped_after_measurement = True
        finally:
            if server is not None and server.poll() is None:
                # Read-only run: the offline tool already checkpointed the complete dataset.
                # Avoid the separately documented legacy socket shutdown endpoint exception.
                server.kill()
                server.wait()
            clients.clear()
            gc.collect()
    if not stopped_after_measurement:
        raise RuntimeError(f"Server exited with {server.returncode}; inspect server.log")
    if any(run["errors"] for run in report["runs"]):
        raise RuntimeError("Measurements contained failed requests; inspect results.json")
    print(f"Results: {result_path}", flush=True)


if __name__ == "__main__":
    main()
