#!/usr/bin/env python3
"""Prepare and evaluate the UInt8 SIFT workload for tenant_perf.cpp."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import struct
import time


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_vectors(np, path):
    with path.open("rb") as stream:
        rows, dimension = struct.unpack("<ii", stream.read(8))
    if rows <= 0 or dimension != 128 or path.stat().st_size != 8 + rows * dimension:
        raise ValueError(f"Invalid DEFAULT UInt8 128D file: {path}")
    return np.memmap(path, dtype=np.uint8, offset=8, shape=(rows, dimension), mode="r")


def write_vectors(path, vectors):
    with path.open("xb") as stream:
        stream.write(struct.pack("<ii", *vectors.shape))
        vectors.tofile(stream)


def truth(np, base, queries, k=5):
    ids = np.empty((len(queries), k), dtype=np.int32)
    distances = np.empty((len(queries), k), dtype=np.float64)
    # Float64 dot products of UInt8 coordinates are exact for these dimensions.
    for qstart in range(0, len(queries), 128):
        q = queries[qstart:qstart + 128].astype(np.float64)
        best_d = np.full((len(q), k), np.inf)
        best_i = np.full((len(q), k), -1, dtype=np.int32)
        for start in range(0, len(base), 32768):
            block = base[start:start + 32768].astype(np.float64)
            d = (np.sum(q * q, axis=1)[:, None] + np.sum(block * block, axis=1)[None, :]
                 - 2 * q @ block.T)
            choices = np.argpartition(d, k - 1, axis=1)[:, :k]
            local_d = np.take_along_axis(d, choices, axis=1)
            candidates_d = np.concatenate((best_d, local_d), axis=1)
            candidates_i = np.concatenate((best_i, choices + start), axis=1)
            selected = np.argsort(candidates_d, axis=1, kind="stable")[:, :k]
            best_d = np.take_along_axis(candidates_d, selected, axis=1)
            best_i = np.take_along_axis(candidates_i, selected, axis=1)
        ids[qstart:qstart + len(q)] = best_i
        distances[qstart:qstart + len(q)] = best_d
        if qstart % 1024 == 0:
            print(f"truth: {qstart}/{len(queries)} queries, {len(base)} vectors", flush=True)
    return ids, distances


def prepare(np, root):
    source = root / "base-prefix.download"
    base_path = root / "sift-first-1m.u8bin"
    if not base_path.exists():
        if source.stat().st_size != 128000008:
            raise ValueError("Expected exactly the first 128000008 bytes of SIFT1B")
        with source.open("rb") as stream:
            rows, dimension = struct.unpack("<ii", stream.read(8))
            if rows != 1000000000 or dimension != 128:
                raise ValueError("Unexpected SIFT1B header")
            with base_path.open("xb") as target:
                target.write(struct.pack("<ii", 1000000, 128))
                while block := stream.read(8 << 20):
                    target.write(block)
    base = read_vectors(np, base_path)
    queries_path = root / "query.public.10K.u8bin"
    queries = read_vectors(np, queries_path)
    if len(queries) != 10000:
        raise ValueError("Expected the public 10K queries")
    manifest = {
        "base_sha256": checksum(base_path), "queries_sha256": checksum(queries_path),
        "representation": "UInt8", "dimension": 128, "metric": "squared L2", "k": 5,
        "partition_seed": 20261104, "partitions": [100000, 200000, 300000, 400000],
        "truth_ties": "one exact top-5 ID set; ties may choose another equally valid ID",
    }
    permutation = np.random.default_rng(manifest["partition_seed"]).permutation(len(base))
    start = 0
    for i, size in enumerate(manifest["partitions"]):
        path = root / f"tenant-{i}.u8bin"
        if not path.exists():
            write_vectors(path, base[permutation[start:start + size]])
        start += size
    for name, count in [("smoke", 1000), ("queries-200", 200)]:
        path = root / f"{name}.u8bin"
        if not path.exists():
            write_vectors(path, base[:count] if name == "smoke" else queries[:count])
    (root / "dataset.json").write_text(json.dumps(manifest, indent=2) + "\n")


def evaluate(np, result, truth_path, output):
    expected = np.load(truth_path)
    truth_ids, truth_dist = expected["ids"], expected["distances"]
    groups = {}
    with result.open() as stream:
        for row in csv.DictReader(stream):
            name, q = row["tenant"], int(row["query"])
            groups.setdefault(name, {}).setdefault(q, []).append(
                (int(row["id"]), float(row["distance"])))
    report = {}
    for name, records in groups.items():
        if set(records) != set(range(len(truth_ids))):
            raise ValueError(f"Incomplete accuracy output for {name}")
        values = []
        for q in range(len(truth_ids)):
            found = records[q]
            if len(found) != 5:
                raise ValueError("Expected five search results")
            strict = len(set(i for i, _ in found if i >= 0) & set(truth_ids[q].tolist()))
            unused = set(range(len(found)))
            tolerant = 0
            for tid, distance in zip(truth_ids[q], truth_dist[q]):
                for rank in sorted(unused):
                    rid, rd = found[rank]
                    if rid >= 0 and (rid == tid or abs(rd - distance) <
                                     1e-4 * (abs(distance) + 1e-4)):
                        unused.remove(rank)
                        tolerant += 1
                        break
            values.append((strict, tolerant))
        values = np.asarray(values)
        report[name] = {
            "queries": len(values),
            "strict_id_recall_at_5": float(values[:, 0].sum() / (5 * len(values))),
            "distance_tolerant_recall_at_5": float(values[:, 1].sum() / (5 * len(values))),
            "first_200_strict": float(values[:200, 0].sum() / (5 * min(200, len(values)))),
            "first_200_tolerant": float(values[:200, 1].sum() / (5 * min(200, len(values)))),
        }
    if not report:
        raise ValueError("No accuracy records")
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("directory", type=Path)
    exact = sub.add_parser("truth")
    exact.add_argument("base", type=Path)
    exact.add_argument("queries", type=Path)
    exact.add_argument("output", type=Path)
    exact.add_argument("--count", type=int)
    ev = sub.add_parser("evaluate")
    ev.add_argument("result", type=Path)
    ev.add_argument("truth", type=Path)
    ev.add_argument("output", type=Path)
    args = parser.parse_args()
    import numpy as np
    if args.command == "prepare":
        prepare(np, args.directory)
    elif args.command == "truth":
        if args.output.exists():
            raise ValueError("Refusing to overwrite ground truth")
        base = read_vectors(np, args.base)
        if args.count is not None:
            if not 5 <= args.count <= len(base):
                raise ValueError("Invalid truth corpus count")
            base = base[:args.count]
        start = time.monotonic()
        ids, distances = truth(np, base, read_vectors(np, args.queries))
        np.savez(args.output, ids=ids, distances=distances, base_sha256=checksum(args.base),
                 query_sha256=checksum(args.queries), count=len(base))
        print(f"Truth completed in {time.monotonic() - start:.1f}s", flush=True)
    else:
        evaluate(np, args.result, args.truth, args.output)


if __name__ == "__main__":
    main()
