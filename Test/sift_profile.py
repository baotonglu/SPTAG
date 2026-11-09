#!/usr/bin/env python3
"""Diagnostic-only GDB stack snapshots when kernel perf events are unavailable.

Runs its own child under GDB. Timings from this run must not enter QPS tables.
"""

import argparse
import json
import os
from pathlib import Path
import queue
import random
import subprocess
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=40)
    parser.add_argument("--frames", type=int, default=12)
    parser.add_argument("--inferior-tty", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command or args.samples < 1 or args.frames < 1:
        raise ValueError("An executable command and positive sample/frame counts are required")
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])
    env = dict(os.environ, OMP_NUM_THREADS="1")
    env["LD_PRELOAD"] = subprocess.check_output(
        ["g++", "-print-file-name=libstdc++.so.6"], text=True).strip()
    with args.output.open("x") as log:
        process = subprocess.Popen(
            ["gdb", "--quiet", "--nx", "--interpreter=mi2", "--args", *command],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env)
        lines = queue.Queue()

        def reader():
            for line in process.stdout:
                log.write(line)
                log.flush()
                lines.put(line)
            lines.put(None)

        thread = threading.Thread(target=reader)
        thread.start()
        token = 0

        def wait(predicate, timeout=120):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                line = lines.get(timeout=max(.01, deadline - time.monotonic()))
                if line is None:
                    raise RuntimeError("GDB exited unexpectedly")
                if predicate(line):
                    return line
            raise TimeoutError("GDB command timed out")

        def send(command, result="done"):
            nonlocal token
            token += 1
            current = str(token)
            process.stdin.write(current + command + "\n")
            process.stdin.flush()
            line = wait(lambda line: line.startswith(current + "^"))
            if not line.startswith(current + "^" + result):
                raise RuntimeError(line.strip())

        try:
            send("-gdb-set pagination off")
            send("-gdb-set confirm off")
            send("-gdb-set print thread-events off")
            send("-gdb-set mi-async on")
            if args.inferior_tty:
                send("-inferior-tty-set " + json.dumps(str(args.inferior_tty)))
            send("-exec-run", "running")
            # Keep sampled runs separate; stop/resume changes scheduling and latency.
            time.sleep(10)
            rng = random.Random(20261104)
            for i in range(args.samples):
                send("-exec-interrupt --all")
                wait(lambda line: line.startswith("*stopped"))
                log.write(f"SAMPLE {i}\n")
                send("-interpreter-exec console " + json.dumps(f"thread apply all bt {args.frames}"))
                send("-exec-continue", "running")
                time.sleep(rng.uniform(.25, .75))
            exited = wait(lambda line: line.startswith("*stopped") and "exited" in line, timeout=300)
            if 'reason="exited-normally"' not in exited:
                raise RuntimeError(f"Profiled command failed: {exited.strip()}")
            send("-gdb-exit", "exit")
            if process.wait(timeout=10):
                raise RuntimeError("GDB failed")
        finally:
            if process.poll() is None:
                # GDB owns the inferior; kill it before closing the debugger.
                try:
                    process.stdin.write("-exec-interrupt --all\n")
                    process.stdin.flush()
                    time.sleep(.2)
                    process.stdin.write('-interpreter-exec console "kill"\n-gdb-exit\n')
                    process.stdin.flush()
                    process.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    process.terminate()
                    process.wait(timeout=10)
            thread.join(timeout=10)


if __name__ == "__main__":
    main()
