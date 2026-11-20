# HyperFlow V0.8 — shared vs sharded reference queues

## Reproduce
Linux/POSIX, GCC 14.2.0, C++20, four-core CPU quota (container).
```sh
g++ -std=c++20 -O3 -Wall -Wextra -Werror -pthread native/shards_v08.cpp -o /tmp/hf08
timeout 20 /tmp/hf08 --test
timeout 90 /tmp/hf08 > /tmp/results_v08.csv
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread native/shards_v08.cpp -o /tmp/hf08san
timeout 20 /tmp/hf08san --test
```

Both shared and sharded strategies use the same implementation: preallocated frame descriptors, borrowed immutable payloads, per-queue mutex/CV, one sending thread, round robin across nonempty queues, sendmsg, buffered decoder. Total reserved in-flight and queued frame bytes <= 65536; sharded capacity is evenly divided among producers. Descriptor storage, payload storage and socket memory are separate. Borrowed payloads must remain immutable and alive until finish; finish must follow producer join. Not a lock-free or kernel zero-copy system.

72 post-fix shuffled trials (3 seeds x 2 repetitions x 3 producer counts x 2 queue strategies x 2 receiver loads), 8000 frames each; all verified bytes and per-producer sequence and EOF. TCP loopback, NODELAY, same pre-generated workload for matched seeds; slow receiver sleeps 500 us every 64 frames. Timing includes thread startup, backpressure and verification. P99 from before submit until verified reception. CPU process-wide. Lock metric measures initial producer acquisition time incl clock overhead and scheduling, excludes CV wait and lock reacquisition. Peak field sums per-shard maxima (upper bound, not instantaneous peak). Group medians across six runs.

| Producers/load | Shared MB/s | Sharded MB/s | Shared lock ns/message | Sharded lock ns/message | Throughput paired wins |
|---|---:|---:|---:|---:|---:|
| 1 normal (identical control) | 195.09 | 567.80 | 44.89 | 55.56 | 4/6 |
| 1 slow (identical control) | 111.94 | 122.17 | 55.02 | 51.32 | 4/6 |
| 2 normal | 385.96 | 471.45 | 177.76 | 43.34 | 4/6 |
| 2 slow | 106.46 | 154.52 | 166.79 | 44.22 | 4/6 |
| 4 normal | 362.04 | 578.84 | 419.73 | 52.46 | 4/6 |
| 4 slow | 93.79 | 128.72 | 426.31 | 48.01 | 5/6 |

Multi-producer lock time improved in all 24 paired trials. Four-producer normal P99 group medians 802.19 / 396.47 us, CPU 2.25 / 2.48 ns/byte, send calls 423.5 / 1349. Single-producer negative control has large throughput variation despite identical configurations; no firm throughput effect size. Max per-trial P99 55.5968 ms, so real-time guarantees not established. Sharding changes both lock domain and available batch capacity; neither factor alone explains all outcomes.

Audit identified possible final-drain race (empty scan concurrent with final enqueue and close). Added epoch recheck before exit, ran all reported benchmarks again. Race was code-review discovery, not reproduced failure. Release and ASan/UBSan test modes passed, each covering ten forced blocked-producer peer disconnect cases and two four-producer trials. No TSAN, remote networking or long-run validation.

Next: longer isolated paired benchmarks and negative controls; aggregate across multiple shards per sendmsg without relaxing reserved bytes; compare dynamic byte quotas and fair scheduling against fixed shards. Keep both implementations until stable throughput and tail latency advantage is verified.

Raw CSV Git blob: dcfb84ffe18876bc665fdb093439fb856ed92f96
Source Git blob: e3cdaade720444613bb6d4e7d8d16c9d0c70f26a
