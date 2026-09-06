"""Claims (README): 'S3-native distributed locking', 'Both writes succeed with ACID
guarantees'. Production (.env) runs DATASHARD_S3_USE_CONDITIONAL_WRITES=false, i.e.
S3PollingLockProvider plus a plain-PUT commit point.

Part 1 (lock level): two polling providers BOTH acquire when one PUT lands after the
other's read-back verification - a few hundred ms of S3 latency on one request.
Part 2 (commit level): given that interleaving plus a slow hint PUT on the first
writer, two Table.append_records() calls both return True and one snapshot is lost
(its data file becomes an orphan that GC will delete).
Control: the same orchestration with conditional writes ON loses nothing.
Timing is injected through a proxy around B's boto3 client; every datashard code path
is the real one.

Since 0.8.0 the polling lock is unreachable without an explicit opt-in: the claims
this probe asserts are (1) the unsafe configuration is REFUSED, (2) an unset flag
auto-detects CAS on a provider that honours it, (3) with CAS nothing is lost. The
polling lost-commit itself is reproduced only as INFO (it is inherent to polling).
"""
import threading
import time
import uuid

import _harness as H

H.quiet_logs()
bucket = "audit-polling"


class DelayedClient:
    """Proxy around a boto3 client: PUTs to `key` wait for `gate`; HEADs of `key` set `headed`."""

    def __init__(self, inner, key, gate, headed):
        self._inner, self._key, self._gate, self._headed = inner, key, gate, headed

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def head_object(self, **kw):
        try:
            return self._inner.head_object(**kw)
        finally:
            if kw.get("Key") == self._key:
                self._headed.set()

    def put_object(self, **kw):
        if kw.get("Key") == self._key:
            self._gate.wait(30)
        return self._inner.put_object(**kw)


def part0_refusal_and_autodetect():
    from datashard import create_table

    H.s3_env(bucket, conditional=False)  # explicit polling, no opt-in
    try:
        create_table(f"refuse_{int(time.time() * 1000)}", H.simple_schema())
        H.report("unsafe-polling-lock-is-refused-without-opt-in", False, "table created with the polling lock")
    except RuntimeError as e:
        H.report("unsafe-polling-lock-is-refused-without-opt-in", "LOST" in str(e), f"refused: {str(e)[:90]}...")
    H.s3_env(bucket, conditional=None)  # unset: datashard must probe the endpoint
    t = create_table(f"detect_{int(time.time() * 1000)}", H.simple_schema())
    H.report("cas-auto-detected-on-a-provider-that-honours-preconditions", t.storage.supports_cas, f"supports_cas={t.storage.supports_cas}")


def part1_lock_level():
    from datashard.lock_provider import S3PollingLockProvider

    s3 = H.s3_env(bucket, conditional=False, allow_unsafe=True)
    key = f"locks/p1-{uuid.uuid4().hex[:8]}.lock"
    gate, headed = threading.Event(), threading.Event()
    a = S3PollingLockProvider(s3, bucket, key, timeout=10)
    b = S3PollingLockProvider(DelayedClient(s3, key, gate, headed), bucket, key, timeout=10)
    res = {}
    tb = threading.Thread(target=lambda: res.__setitem__("b", b.acquire()))
    tb.start()
    headed.wait(10)  # B saw "no lock object" ...
    res["a"] = a.acquire()  # ... A creates it and verifies ...
    gate.set()  # ... then B's delayed PUT lands and B verifies its own id
    tb.join(20)
    both = bool(res.get("a")) and bool(res.get("b")) and a.is_locked and b.is_locked
    print(f"  info: polling lock (opt-in only): A acquired={res.get('a')} B acquired={res.get('b')}; "
          f"both hold the lock simultaneously={both} - inherent to check-then-write locking")
    a.release()
    b.release()


def part2_commit_level(conditional):
    s3 = H.s3_env(bucket, conditional=conditional, allow_unsafe=not conditional)
    from datashard import create_table, load_table

    schema = H.simple_schema()
    tpath = f"lost_{'cas' if conditional else 'poll'}_{int(time.time() * 1000)}"
    tA = create_table(tpath, schema)
    tA.append_records([{"id": 0, "name": "base", "value": 0.0}], schema)
    tB = load_table(tpath)
    key = tA.metadata_manager.lock_provider.key
    gate, headed, b_done = threading.Event(), threading.Event(), threading.Event()
    tB.metadata_manager.lock_provider.s3 = DelayedClient(tB.storage.s3, key, gate, headed)
    mmA = tA.metadata_manager
    orig_hint = mmA._write_hint_at_commit_point

    def slow_hint(*a, **k):
        gate.set()  # A passed its ownership fence; B's delayed lock PUT now lands
        b_done.wait(20)  # A's hint PUT suffers a latency spike while B commits
        return orig_hint(*a, **k)

    mmA._write_hint_at_commit_point = slow_hint
    res = {}

    def run(tag, t):
        try:
            res[tag] = t.append_records([{"id": 1 if tag == "A" else 2, "name": tag, "value": 1.0}], schema)
        except Exception as e:  # noqa: BLE001
            res[tag] = f"{type(e).__name__}: {str(e)[:70]}"
        if tag == "B":
            b_done.set()

    thB = threading.Thread(target=run, args=("B", tB))
    thB.start()
    headed.wait(10)
    thA = threading.Thread(target=run, args=("A", tA))
    thA.start()
    thA.join(120)
    thB.join(120)
    rows = load_table(tpath).row_count()
    data_keys = H.count_s3_keys(s3, bucket, f"{tpath}/data/")
    succeeded = sum(1 for v in res.values() if v is True)
    ok = rows == 1 + succeeded
    msg = (f"A returned {res.get('A')}, B returned {res.get('B')}; rows visible={rows}, expected {1 + succeeded}; "
           f"data files on S3={data_keys}" + ("" if ok else f" -> {data_keys - rows} committed file(s) orphaned"))
    if conditional:
        H.report("every-commit-that-returned-True-is-visible-(conditional-lock)", ok, msg)
    else:
        print(f"  info: polling lock (opt-in only), lost commit reproduced={not ok}: {msg}")


part0_refusal_and_autodetect()
part1_lock_level()
part2_commit_level(conditional=False)
part2_commit_level(conditional=True)
H.finish()
