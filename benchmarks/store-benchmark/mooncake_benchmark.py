#!/usr/bin/env python3
"""Simple benchmarking tool for Mooncake Store.

This script performs repeated Put/Get operations against a running
MooncakeDistributedStore instance. It is loosely inspired by
``redis-benchmark`` and allows you to specify the number of requests
and concurrency level.

Example usage:
    python3 mooncake_benchmark.py -n 10000 -c 32 --operation put

Environment variables like ``LOCAL_HOSTNAME`` or ``MC_METADATA_SERVER``
are used to configure the store if no command line option is provided.
"""

import argparse
import os
import random
import threading
import time

from mooncake.store import MooncakeDistributedStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mooncake Store benchmark")
    parser.add_argument(
        "-n", "--requests", type=int, default=10000,
        help="Total number of requests"
    )
    parser.add_argument(
        "-c", "--clients", type=int, default=50,
        help="Number of concurrent clients"
    )
    parser.add_argument(
        "-s", "--size", type=int, default=1024,
        help="Value size for PUT in bytes"
    )
    parser.add_argument(
        "-o", "--operation", choices=["put", "get", "mixed"], default="put",
        help="Benchmark operation"
    )
    parser.add_argument("--protocol", default=os.getenv("PROTOCOL", "tcp"))
    parser.add_argument("--device_name", default=os.getenv("DEVICE_NAME", ""))
    parser.add_argument(
        "--local_hostname", default=os.getenv("LOCAL_HOSTNAME", "localhost")
    )
    parser.add_argument(
        "--metadata_server",
        default=os.getenv("MC_METADATA_SERVER", os.getenv("METADATA_ADDR", "127.0.0.1:2379")),
    )
    parser.add_argument(
        "--master_server_address",
        default=os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    parser.add_argument(
        "--global_segment_size", type=int, default=3200 * 1024 * 1024
    )
    parser.add_argument(
        "--local_buffer_size", type=int, default=512 * 1024 * 1024
    )
    return parser.parse_args()


def setup_store(args: argparse.Namespace) -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    ret = store.setup(
        args.local_hostname,
        args.metadata_server,
        args.global_segment_size,
        args.local_buffer_size,
        args.protocol,
        args.device_name,
        args.master_server_address,
    )
    if ret != 0:
        raise RuntimeError(f"Store setup failed with code {ret}")
    return store


def worker_put(store: MooncakeDistributedStore, keys: list[str], value: bytes) -> None:
    for k in keys:
        store.put(k, value)


def worker_get(store: MooncakeDistributedStore, keys: list[str]) -> None:
    for k in keys:
        store.get(k)


def run_benchmark(args: argparse.Namespace) -> None:
    store = setup_store(args)

    # Keys are distributed across threads to avoid contention
    total = args.requests
    per_thread = total // args.clients
    remainder = total % args.clients

    # Pre-generate keys
    all_keys = [f"key_{i}" for i in range(total)]
    value = bytes(random.getrandbits(8) for _ in range(args.size))

    # Pre-fill data for GET benchmark
    if args.operation in {"get", "mixed"}:
        for k in all_keys:
            store.put(k, value)

    threads = []
    start = time.time()

    def target(start_idx: int, count: int) -> None:
        sub_keys = all_keys[start_idx:start_idx + count]
        if args.operation == "put":
            worker_put(store, sub_keys, value)
        elif args.operation == "get":
            worker_get(store, sub_keys)
        else:  # mixed
            for k in sub_keys:
                store.put(k, value)
                store.get(k)

    index = 0
    for i in range(args.clients):
        cnt = per_thread + (1 if i < remainder else 0)
        t = threading.Thread(target=target, args=(index, cnt))
        threads.append(t)
        index += cnt
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start
    ops = args.requests if args.operation != "mixed" else args.requests * 2
    print(
        f"\nBenchmark results:\n"
        f"Operation: {args.operation}\n"
        f"Clients: {args.clients}\n"
        f"Requests: {args.requests}\n"
        f"Value size: {args.size} bytes\n"
        f"Total time: {duration:.2f} seconds\n"
        f"Throughput: {ops / duration:.2f} ops/sec"
    )


if __name__ == "__main__":
    run_benchmark(parse_args())
