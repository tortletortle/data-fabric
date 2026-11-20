# HyperFlow V0.7: buffered receive and concurrency validation

The V0.6 C++ Linux/POSIX sender remains unchanged. V0.7 is a benchmark driver and incremental decoder prototype, not a new public SDK. The old sender is included directly so baseline and treatment share the same code.

## Reproduction

From repository root:
```sh
g++ -std=c++17 -O3 -Wall -Wextra -Werror -pthread native/hyperio_v07.cpp -o /tmp/hyperio_v07
timeout 20 /tmp/hyperio_v07 --test
timeout 90 /tmp/hyperio_v07 > /tmp/results_v07.csv
g++ -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread native/hyperio_v07.cpp -o /tmp/hyperio_v07_san
timeout 20 /tmp/hyperio_v07_san --test
```

Environment: GCC 14.2.0, Linux container, CPU quota equivalent to 4 cores; loopback TCP, TCP_NODELAY enabled, 64 KiB sender frame-byte cap, 64 KiB buffered receiver. Seeded payloads are generated before timing. Timing includes thread startup, submit blocking, decoding and byte checking; CPU is process-wide. P99 is nearest-rank per trial from before submit until verified receive on the same clock. Reported group medians are across six trials, not pooled percentiles. The pre-generated workload and other overheads are not included in the sender cap.

96 shuffled trials, 3 seeds x 2 repeats x 2 send batch limits x 2 receiver strategies x 2 producer counts x 2 receiver loads. Each run verifies 8000 frames, per-producer order and bytes, and exact EOF. Slow reader sleeps 500 us every 64 messages. All 768000 messages passed. Release and ASan/UBSan self-tests each passed 10 forced stalled-producer disconnect cases and 2 four-producer decoding trials. Distinct tests from benchmarks, sanitizer success is not a race detector.

## Results: batch limit 32, medians

| Producers/load | Frame read MB/s | Buffered MB/s | Frame P99 us | Buffered P99 us | Frame CPU ns/byte | Buffered CPU ns/byte | Buffered throughput pair wins |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 normal | 1127.28 | 1374.77 | 121.19 | 133.87 | 1.32 | 1.06 | 6/6 |
| 1 slow | 111.27 | 146.66 | 1464.48 | 1256.66 | 1.92 | 1.39 | 4/6 |
| 4 normal | 140.90 | 167.57 | 334.47 | 272.41 | 2.63 | 2.43 | 2/6 |
| 4 slow | 118.65 | 87.78 | 1451.87 | 1378.67 | 3.67 | 3.06 | 3/6 |

For 1 normal producer, median recv calls 14624.5 -> 447. Buffered decoder reduces syscall counts but does not eliminate copying (partial-frame compaction and kernel copy). With batch limit 1, gains are not consistent; 4-producer results are noisy. 4 producers share a single locked sender; contention is a hypothesis, not established causality. Need measured lock wait / context switch / CPU affinity controls and compare per-producer SPSC queues against existing shared ring with matched capacity and fairness. No remote-network, long-duration, TSAN or reconnect validation. No comparison with V0.6 absolute throughput due to changed benchmark.

Raw data: results_v07.csv (Git blob 52ebc6d60addda5f48adcd29a60594fe800a143a). Source: hyperio_v07.cpp (Git blob 7c933a2983afd3675a9efb54385d107ee94788ad).
