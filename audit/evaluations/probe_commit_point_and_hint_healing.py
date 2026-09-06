"""Claim (0.10, #86): the commit point is the exclusive creation of
metadata/v{N}.metadata.json, and metadata/version-hint.text is only a pointer.

Consequences this probe tries to break:
  (1) a failure AT the commit point leaves no metadata file behind;
  (2) a failure AFTER it (the hint write) is NOT a failed commit - the rows are
      durable, and the next reader heals the hint;
  (3) a hint that lags a durable commit never hides rows, from a reader OR a writer;
  (4) two writers racing for the same version: exactly one wins;
  (5) a lost hint is recovered from the listing without guessing (versions are
      unique by construction, so the pre-0.10 ambiguity cannot arise).
"""
import glob
import json
import multiprocessing as mp
import os
import tempfile

import _harness as H

H.local_env()
H.quiet_logs()
from datashard import create_table, load_table  # noqa: E402


class _NoLock:
    """The metadata lock removed, so ONLY the commit point can prevent a lost update."""

    def acquire(self):
        return True

    def release(self):
        return None

    def is_held(self):
        return True


def racer(p, barrier, out, weaken_commit_point=False):
    """Commit a property change from a base every racer shares, with no lock held.

    weaken_commit_point replaces the exclusive create with a plain write: the control
    run for this probe's oracle, which must then see SEVERAL winners (a check that
    cannot go green cannot go red).
    """
    import logging

    logging.getLogger("datashard").setLevel(logging.CRITICAL)
    import datashard

    tt = datashard.load_table(p)
    tt.metadata_manager.lock_provider = _NoLock()
    if weaken_commit_point:
        tt.storage.create_exclusive = tt.storage.write_file
    base = tt.metadata_manager.refresh()
    new = tt.metadata_manager._dict_to_metadata(tt.metadata_manager._metadata_to_dict(base))
    new.properties["racer"] = str(os.getpid())
    barrier.wait(30)
    try:
        tt.metadata_manager.commit(base, new)
        out.put(("won", base.current_snapshot_id))
    except Exception as e:  # noqa: BLE001
        out.put((type(e).__name__, base.current_snapshot_id))


if __name__ == "__main__":

    tmp = tempfile.mkdtemp(prefix="audit_commitpoint_")
    path = os.path.join(tmp, "t")
    schema = H.simple_schema()
    HINT = os.path.join(path, "metadata", "version-hint.text")


    def versions():
        return sorted(int(os.path.basename(f)[1:].split(".")[0]) for f in glob.glob(os.path.join(path, "metadata", "v*.metadata.json")))


    t = create_table(path, schema)
    t.append_records([{"id": 1, "name": "A", "value": 1.0}], schema)

    # (1) the commit point itself fails -> nothing new is visible, nothing left behind
    before = versions()
    real_create = t.storage.create_exclusive
    t.storage.create_exclusive = lambda p, c: (_ for _ in ()).throw(OSError("simulated failure at the commit point"))
    try:
        t.append_records([{"id": 2, "name": "B", "value": 2.0}], schema)
        outcome = "commit unexpectedly succeeded"
    except Exception as e:  # noqa: BLE001
        outcome = f"raised {type(e).__name__}"
    finally:
        t.storage.create_exclusive = real_create
    H.report(
        "commit-point-failure-leaves-no-metadata-file",
        versions() == before and load_table(path).row_count() == 1,
        f"{outcome}; versions {before} -> {versions()}; rows={load_table(path).row_count()}",
    )

    # (2) the hint write fails AFTER the commit point: the commit is durable anyway
    real_write = t.storage.write_file


    def failing_hint(p, content):
        if p == t.metadata_manager.HINT_PATH:
            raise OSError("simulated: hint write failed after the commit point")
        return real_write(p, content)


    t.storage.write_file = failing_hint
    try:
        committed = t.append_records([{"id": 2, "name": "B", "value": 2.0}], schema)
        err = None
    except Exception as e:  # noqa: BLE001
        committed, err = False, f"{type(e).__name__}: {e}"
    finally:
        t.storage.write_file = real_write
    hint_after = open(HINT).read().strip()
    rows_now = load_table(path).row_count()
    hint_healed = open(HINT).read().strip()
    H.report(
        "hint-failure-after-the-commit-point-does-not-fail-the-commit",
        committed and rows_now == 2,
        f"append returned {committed} (err={err}); rows readable after={rows_now}; "
        f"hint lagged at {hint_after}, healed to {hint_healed} by the reader",
    )

    # (3) a deliberately stale hint hides nothing, for readers or writers
    open(HINT, "w").write("1")
    reader_rows = load_table(path).row_count()
    open(HINT, "w").write("1")
    writer = load_table(path)
    writer.append_records([{"id": 3, "name": "C", "value": 3.0}], schema)
    after_rows = load_table(path).row_count()
    H.report(
        "a-stale-hint-hides-no-committed-rows-from-readers-or-writers",
        reader_rows == 2 and after_rows == 3,
        f"reader saw {reader_rows} rows with the hint pinned at v1 (expected 2); "
        f"a writer with the same stale hint committed and the table then has {after_rows} rows (expected 3)",
    )

    # (4) two processes racing for the same version: exactly one wins
    ctx = mp.get_context("spawn")
    q, barrier = ctx.Queue(), ctx.Barrier(6)
    procs = [ctx.Process(target=racer, args=(path, barrier, q)) for _ in range(6)]
    before_versions = versions()
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join(120)
    results = [q.get() for _ in range(6)]
    outcomes = [r[0] for r in results]
    bases = {r[1] for r in results}
    wins = outcomes.count("won")
    new_versions = [v for v in versions() if v not in before_versions]
    ctrl_q, ctrl_barrier = ctx.Queue(), ctx.Barrier(6)
    ctrl_procs = [ctx.Process(target=racer, args=(path, ctrl_barrier, ctrl_q, True)) for _ in range(6)]
    ctrl_before = versions()
    for pr in ctrl_procs:
        pr.start()
    for pr in ctrl_procs:
        pr.join(120)
    ctrl = [ctrl_q.get()[0] for _ in range(6)]
    ctrl_added = [v for v in versions() if v not in ctrl_before]
    H.report(
        "control: without the exclusive create, several writers overwrite one version",
        ctrl.count("won") > 1 and len(ctrl_added) <= 1,
        f"with create_exclusive replaced by a plain write: outcomes={sorted(ctrl)}, versions added="
        f"{ctrl_added} - {ctrl.count('won')} writers believed they committed the same version, which is "
        f"what the real commit point prevents above",
    )
    H.report(
        "one-winner-per-version-even-with-the-lock-removed",
        wins == 1 and len(bases) == 1 and len(new_versions) == 1 and load_table(path).row_count() == 3,
        f"6 processes commit the SAME base (snapshot ids seen: {bases}) with the metadata lock "
        f"replaced by a no-op, so only the exclusive create of v{{N+1}} can fence them: "
        f"outcomes={sorted(outcomes)}; versions added={new_versions}; rows={load_table(path).row_count()}",
    )

    # (5) a lost hint is recovered from the listing, and the recovered version is the real one
    expected = max(versions())
    os.remove(HINT)
    recovered = load_table(path)
    info = recovered.metadata_manager._current_version_info()
    doc = json.load(open(os.path.join(path, "metadata", info[1])))
    H.report(
        "a-lost-hint-is-recovered-from-the-listing-without-guessing",
        info[0] == expected and doc["format-version"] == 2 and recovered.row_count() == 3,
        f"recovered v{info[0]} (highest on disk v{expected}); rows={recovered.row_count()}; hint rewritten to "
        f"{open(HINT).read().strip() if os.path.exists(HINT) else 'MISSING'}",
    )
    H.finish()
