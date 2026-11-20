"""Bounded TCP sender and reproducible loopback probe (Python 3/Linux)."""
import collections, hashlib, random, socket, struct, threading, time
import statistics, json

H = struct.Struct('!II')

class Sender:
    def __init__(self, sock, capacity=65536, batch=32):
        if capacity <= H.size or batch < 1:
            raise ValueError('invalid limits')
        self.sock, self.cap, self.batch = sock, capacity, batch
        self.cv = threading.Condition()
        self.q = collections.deque()
        self.used = self.peak = self.waits = self.calls = 0
        self.closed = False
        self.error = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def submit(self, seq, payload):
        if not isinstance(payload, bytes):
            raise TypeError('immutable bytes required')
        size = H.size + len(payload)
        if size > self.cap:
            raise ValueError('frame exceeds capacity')
        with self.cv:
            while self.used + size > self.cap and not self.closed:
                self.waits += 1
                self.cv.wait()
            if self.closed:
                raise RuntimeError('sender closed') from self.error
            self.q.append((H.pack(seq, len(payload)), payload))
            self.used += size
            self.peak = max(self.peak, self.used)
            self.cv.notify_all()

    def run(self):
        try:
            while True:
                with self.cv:
                    self.cv.wait_for(lambda: self.q or self.closed)
                    if not self.q:
                        break
                    frames = [self.q.popleft() for _ in range(min(self.batch, len(self.q)))]
                views = collections.deque(memoryview(b) for f in frames for b in f if b)
                size = sum(map(len, views))
                while views:
                    n = self.sock.sendmsg(list(views)[:128]); self.calls += 1
                    if n <= 0:
                        raise ConnectionError('send returned zero')
                    while views and n >= len(views[0]):
                        n -= len(views.popleft())
                    if n:
                        views[0] = views[0][n:]
                with self.cv:
                    self.used -= size
                    self.cv.notify_all()
            self.sock.shutdown(socket.SHUT_WR)
        except Exception as exc:
            with self.cv:
                self.error, self.closed = exc, True
                self.cv.notify_all()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def finish(self):
        with self.cv:
            self.closed = True
            self.cv.notify_all()
        self.thread.join()
        if self.error:
            raise RuntimeError('transmission failed') from self.error
        assert self.used == 0


def read_exact(sock, view):
    while view:
        n = sock.recv_into(view)
        if not n:
            raise EOFError('truncated frame')
        view = view[n:]


def probe(seed, batch, slow=False, count=12000):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0)); listener.listen(1)
    tx = socket.create_connection(listener.getsockname(), timeout=5)
    rx, _ = listener.accept(); listener.close()
    tx.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16384)
    rx.settimeout(5); tx.settimeout(5)
    result = {}; errors = []
    def receive():
        digest = hashlib.sha256(); header = bytearray(H.size); buf = bytearray(8192)
        try:
            for seq in range(count):
                read_exact(rx, memoryview(header)); i, size = H.unpack(header)
                if i != seq or size > len(buf):
                    raise ValueError('invalid frame')
                view = memoryview(buf)[:size]; read_exact(rx, view)
                digest.update(header); digest.update(view)
                if slow and seq % 32 == 0:
                    time.sleep(.001)
            if rx.recv(1):
                raise ValueError('unexpected trailing bytes')
            result['digest'] = digest.hexdigest()
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            rx.close()
    reader = threading.Thread(target=receive); reader.start()
    sender = Sender(tx, batch=batch)
    rng = random.Random(seed); digest = hashlib.sha256(); total = 0
    start = time.perf_counter(); cpu = time.process_time()
    try:
        for i in range(count):
            p = rng.randbytes(rng.choice((64, 128, 512, 2048, 8192)))
            digest.update(H.pack(i, len(p))); digest.update(p); total += len(p)
            sender.submit(i, p)
    finally:
        try:
            sender.finish()
        finally:
            tx.close(); reader.join()
    elapsed = time.perf_counter() - start
    assert not errors and result.get('digest') == digest.hexdigest(), errors
    assert sender.peak <= sender.cap and sender.used == 0
    return dict(seed=seed, batch=batch, slow=slow, messages=count,
                MBps=total/elapsed/1e6, cpu_s=time.process_time()-cpu,
                peak_bytes=sender.peak, waits=sender.waits, calls=sender.calls,
                verified=True)

if __name__ == '__main__':
    rows = [probe(s, b, slow) for slow in (False, True) for b in (1, 32) for s in (11, 29, 47)]
    print(json.dumps(rows, indent=2))
