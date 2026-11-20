# HyperFlow V0.6 — native TCP research prototype

Linux/POSIX C++17, GCC 14.2.0. The current environment has no Rust compiler; this is a native mechanism validation, not a Rust port. V0.5 Python remains in the repository.

## Reproduce

From repository root:

```sh
g++ -std=c++17 -O3 -Wall -Wextra -Werror -pthread native/hyperio.cpp -o /tmp/hyperio
/tmp/hyperio --test
/tmp/hyperio > native/results-new.csv
g++ -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread native/hyperio.cpp -o /tmp/hyperio-san
/tmp/hyperio-san --test
```

## Scope and evidence

- Fixed byte ring and preallocated frame-length metadata; payload is copied into the ring once. In-flight bytes remain reserved until the batch completes. This is not kernel zero-copy, nor a lock-free implementation.
- TCP framing: big-endian 32-bit sequence and payload length. sendmsg handles short positive writes and EINTR. Test forces writes to at most 13 bytes per call, exercises ring wrap, zero-length messages, oversize rejection and peer shutdown. Controlled segmentation is not proof that the OS returned short writes. Release and ASan/UBSan self-tests passed.
- 72 TCP loopback runs: 3 seeds x 3 repetitions x 2 batch limits x 2 receiver modes x 2 TCP_NODELAY settings, shuffled using seed 20261120. Each run checks all 12000 sequence numbers and payload bytes and final EOF. All completed. The slow receiver sleeps 1 ms per 32 messages. The 64 KiB cap includes queued and in-flight frame bytes only; descriptor storage, socket buffers, generator and process memory are separate.
- Median payload MB/s (batch 1 / 32): normal default 72.46 / 78.33; normal NODELAY 278.36 / 254.93; slow default 44.49 / 40.45; slow NODELAY 45.07 / 44.83. Peak single run 360.282 MB/s. Batch benefits are workload-dependent.
- Timed region includes data generation, verification and thread scheduling. CPU per byte, P99, remote networking, bidirectional/multi-producer stress, forced blocked-producer disconnect, and delivery acknowledgments not verified. Send completion means accepted by local socket, not confirmed application delivery. No claim of production readiness or improvement over Python under matched conditions.

Next: pre-generate workload, isolate socket and scheduling effects, add latency and CPU cost, validate failure propagation under concurrent producers, then port tested invariants to Rust.
