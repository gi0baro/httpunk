"""Test harness config.

Wraps every `@pytest.mark.tonio` test in a tonio deadline so a stuck coroutine
fails fast (with a clear message) instead of hanging the shared runtime — the
tonio pytest plugin runs all tests on one long-lived runtime, so a single hung
teardown would otherwise wedge the whole run.
"""

import functools

import pytest
from tonio.colored.time import timeout as _tonio_timeout


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
