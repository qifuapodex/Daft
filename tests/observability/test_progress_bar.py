from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading

import pytest

import daft
from daft.runners.progress_bar import SwordfishProgressBar


# Non-regression test for progress bar truncating UTF-8 pipeline names correctly.
# See: https://github.com/Eventual-Inc/Daft/actions/runs/21921434809
@pytest.mark.parametrize(
    "col_name",
    [
        "ñ" * 10,  # 20 bytes UTF-8 (each ñ = 2 bytes), truncation at byte 15 splits a char
        "日本語カラム名テスト",  # 30 bytes UTF-8 (each CJK char = 3 bytes)
        "🎉🎊🎈🎁🎂🎃",  # 24 bytes UTF-8 (each emoji = 4 bytes)
        "café_résumé_naïve",  # mixed ASCII and 2-byte chars
    ],
    ids=["two_byte_chars", "three_byte_cjk", "four_byte_emoji", "mixed_ascii_multibyte"],
)
def test_progress_bar_truncates_multibyte_utf8_pipeline_names(col_name):
    """Progress bar should not panic when truncating pipeline names with multi-byte UTF-8."""
    df = daft.from_pydict({col_name: [1.0, 2.0, 3.0]})
    # col + col is an "interesting" expression that survives optimizer constant-folding,
    # causing ProjectOperator to use the expression display name as the pipeline name.
    df = df.with_column(col_name, daft.col(col_name) + daft.col(col_name))
    result = df.collect()
    assert result.to_pydict()[col_name] == [2.0, 4.0, 6.0]


# Non-regression test for fd leaks in Jupyter: writing to stdout from native threads
# leaks an ipykernel zmq pipe (~2 fds) per write on Python 3.13+, because each call
# into Python from a native thread can get a fresh thread identity. All tqdm writes
# must therefore happen on a single long-lived Python thread, never the caller's.
# See: https://github.com/Eventual-Inc/Daft/issues/7253
def test_swordfish_progress_bar_writes_from_single_stable_thread():
    write_threads = set()

    class RecordingTqdm:
        def __init__(self, *args, **kwargs):
            write_threads.add(threading.get_ident())

        def set_description_str(self, desc):
            write_threads.add(threading.get_ident())

        def close(self):
            write_threads.add(threading.get_ident())

    bar = SwordfishProgressBar()
    bar.tqdm_mod = RecordingTqdm
    pb_id = bar.make_new_bar("🗡️ 🐟 test: {elapsed} {desc}")

    # Simulate updates arriving from short-lived foreign threads, each with a
    # distinct Python thread identity, as happens with native threads on 3.13+.
    caller_threads = []
    for i in range(5):
        t = threading.Thread(target=bar.update_bar, args=(pb_id, f"message {i}"))
        t.start()
        t.join()
        caller_threads.append(t.ident)
    bar.close()

    assert len(write_threads) == 1
    assert write_threads.isdisjoint(caller_threads)


# Non-regression test for a deadlock between the CPython GIL and indicatif's internal
# MultiProgress lock. `IndicatifLogger::log` suspends the progress bars around the inner
# logger, which holds indicatif's write lock while `pyo3_log` acquires the GIL. Meanwhile
# `PyNativeExecutor::run` takes that same lock (via `MultiProgress::add`, when it builds the
# bars for a query) with the GIL already held, so the two lock orders invert and both threads
# wedge forever. It needs something logging frequently off a non-Python thread to hit, which
# the dashboard subscriber does once a dashboard server is up.
#
# Runs in a subprocess because `dashboard.launch()` starts a process-global server that is
# never torn down, and because a deadlocked run can only be detected as a timeout.
_GIL_DEADLOCK_REPRO = textwrap.dedent("""
    import tempfile

    import daft
    from daft.subscribers.dashboard import launch

    launch(noop_if_initialized=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(400):
            df = daft.from_pydict({"id": list(range(i * 100, (i + 1) * 100))})
            df.write_parquet(f"{tmpdir}/file_{i}.parquet")
    print("no deadlock")
""")


def test_indicatif_logger_does_not_deadlock_against_the_gil():
    """Rust logs emitted while the progress bar is live must not deadlock the executor."""
    env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "true"}
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _GIL_DEADLOCK_REPRO],
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("writes deadlocked between the GIL and the indicatif progress bar lock")
    assert proc.returncode == 0, proc.stderr
    assert "no deadlock" in proc.stdout
