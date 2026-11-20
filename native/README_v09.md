# HyperFlow V0.9 — cross-shard aggregation

## Reproduction
Linux/POSIX C++20, GCC 14.2.0-19; container CPU quota 4 cores, effective cpuset 0-4.
```sh
g++ -std=c++20 -O3 -Wall -Wextra -Werror -pthread native/aggregate_v09.cpp -o /tmp/hf09
timeout 20 /tmp/hf09 --test
timeout 65 /tmp/hf09 > /tmp/results_v09.csv
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread native/aggregate_v09.cpp -o /tmp/hf09san
timeout 20 /tmp/hf09san --test
```

Both arms use the V0.8 descriptor queues with fixed equal shard quotas, TCP_NODELAY, borrowed immutable payloads, same buffered receiver and total 65536-byte reserved frame cap. The only intended behavioral change is that aggregation collects multiple shards in a batch instead of stopping at one; global batch limit 32 messages and up to 64 iovecs. Per-shard reservation survives partial writes and is released after the full batch; round-robin cursor advances past last selected shard. One-producer arms are functionally equivalent negative controls, not different algorithms. Payloads remain alive through finish. Not a kernel zero-copy implementation.

48 shuffled trials (3 seeds x 2 repeats x 2 producer counts x 2 strategies x 2 receiver loads); 24000 pre-generated variable-length frames each, all 1152000 checked for bytes and per-producer sequence and exact EOF. 500 us artificial receive pause every 64 frames in slow mode. Timing includes thread startup, producer blocking, transmission and verification; excludes payload generation. CPU process-wide, latency from pre-submit to verified receive; medians of per-trial P99. Capacity metric is sum of per-shard maxima (upper bound), not whole-process memory.

| Producers/load | Baseline MB/s | Aggregate MB/s | Baseline CPU ns/byte | Aggregate CPU ns/byte | Baseline P99 us | Aggregate P99 us | Throughput pair wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 normal (negative control) | 504.12 | 615.84 | 1.268 | 1.220 | 354.88 | 339.52 | 2/6 |
| 1 slow (negative control) | 117.04 | 92.56 | 1.821 | 1.788 | 1625.67 | 2469.21 | 0/6 |
| 4 normal | 539.25 | 598.10 | 2.625 | 2.051 | 541.76 | 402.71 | 5/6 |
| 4 slow | 138.56 | 147.24 | 3.276 | 3.012 | 1600.80 | 1555.86 | 5/6 |

Four normal: median send calls 4030.5 -> 1719.5, recv calls 3269.5 -> 1620, CPU cost improved 6/6, P99 4/6. Four slow: median send calls 4036 -> 1934, CPU improved 4/6, P99 3/6. Send/recv calls improved every four-producer matched pair (12/12). Negative control throughput variability and order effect prevent claiming a stable throughput gain. Max four-producer aggregate trial P99 2940.06 us. No real-time guarantee. Release and ASan/UBSan test modes each passed 20 forced stalled-producer disconnect trials and 2 four-producer correctness trials, separate from main benchmark. Sanitizers are not TSan. Self-test throughput is not included in reported benchmark.

Fixed per-shard quota remains; dynamic capacity allocation deliberately deferred to isolate aggregation. Fairness under heterogeneous rates and payload sizes, TSan, multi-device, long-duration, application delivery acknowledgment and Rust port remain unverified. Next isolate dynamic quota design with a shared global cap, starvation checks and matched negative controls.

Source blob 8bf9856f93348e2eae71812d5a646b765f370cb3
CSV blob 6b24ed65fd7286b40aec4df168af4f12ee4065ac
