"""Test harness config.

httpunk runs on two backends. The tonio backend needs free-threaded CPython >= 3.14,
so on any other interpreter (GIL enabled, or tonio not installed) the tonio-dependent
test modules can't even import — `pytest_ignore_collect` skips them there, leaving the
asyncio + pure-Rust-codec tests to run everywhere. On a tonio-capable interpreter the
full suite runs, wrapped in a tonio deadline (below).
"""

import functools
import re
import sys

import pytest


try:
    from tonio.colored.time import timeout as _tonio_timeout

    _tonio_installed = True
except ImportError:
    _tonio_timeout = None
    _tonio_installed = False

# tonio needs the GIL disabled; `sys._is_gil_enabled` is 3.13+, and older builds are
# always GIL-on, so default True there.
_gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
TONIO_OK = _tonio_installed and not _gil_enabled

# A test module needs tonio if it imports it or constructs `TonioBackend` (matched on
# source so new files need no registration; `AutoServerProtocol` etc. mention "tonio"
# only in prose, which these patterns don't hit).
_NEEDS_TONIO = re.compile(r"from tonio|import tonio|_backend\.tonio|\bTonioBackend\b")


def pytest_ignore_collect(collection_path, config):
    if TONIO_OK or collection_path.suffix != ".py":
        return None
    return True if _NEEDS_TONIO.search(collection_path.read_text()) else None


if TONIO_OK:
    # Production has NO default backend — `_backend.resolve(None)` raises. Most tests
    # construct connections without an explicit `backend=`, so patch `resolve` ONCE,
    # session-wide, to default a `None` backend to tonio. (A single module-level patch
    # rather than an autouse fixture — the tonio pytest plugin doesn't play well with
    # per-test fixtures.) Drivers call `resolve` module-qualified, so this reaches them
    # all; the asyncio tests pass an explicit backend, so their `None` branch never fires.
    from httpunk import _backend

    _real_resolve = _backend.resolve
    _backend.resolve = lambda backend: _real_resolve(_backend.Backend.tonio if backend is None else backend)

    _TONIO_TEST_TIMEOUT = 6.0  # seconds; loopback tests finish in well under 1s

    def pytest_collection_modifyitems(config, items):
        for item in items:
            if not isinstance(item, pytest.Function) or item.get_closest_marker("tonio") is None:
                continue
            orig = item.obj
            if getattr(orig, "__tonio_timeout_wrapped__", False):
                continue

            @functools.wraps(orig)
            async def wrapped(*args, _orig=orig, _name=item.nodeid, **kwargs):
                # tonio's `timeout` returns (result, completed): completed=False means
                # the deadline hit — it aborts so `run_until_complete` returns instead
                # of hanging the shared runtime forever.
                result, completed = await _tonio_timeout(_orig(*args, **kwargs), _TONIO_TEST_TIMEOUT)
                if not completed:
                    import gc
                    import types

                    stuck = []
                    for obj in gc.get_objects():
                        if not isinstance(obj, types.CoroutineType):
                            continue
                        frame = obj.cr_frame  # None once the coroutine has finished
                        if frame is None:
                            continue
                        stuck.append(
                            f"{obj.cr_code.co_qualname} @ {frame.f_code.co_filename.split('/')[-1]}:{frame.f_lineno}"
                        )
                    raise TimeoutError(
                        f"tonio test {_name} exceeded {_TONIO_TEST_TIMEOUT}s (teardown hang?)\n"
                        f"suspended coroutines:\n  " + "\n  ".join(stuck or ["<none>"])
                    )
                return result

            wrapped.__tonio_timeout_wrapped__ = True
            item.obj = wrapped
