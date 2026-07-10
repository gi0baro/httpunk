"""Runtime backends. Only this layer talks to a specific async runtime; the
drivers (httpunk.h1/h2/util) are backend-agnostic — they take a `backend` and use
only the seam it exposes.

There is **no default backend**: `tonio` needs free-threaded CPython >= 3.14, while
`asyncio` is available everywhere (incl. GIL builds), so the caller must choose.
Selection is explicit — a `Backend` enum member (resolved to an instance lazily, so
picking `asyncio` never imports tonio) or a backend instance. This module imports no
backend eagerly; `Backend.<member>.create()` does the import on demand.
"""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Any


class Backend(enum.Enum):
    """The available runtime backends. `create()` instantiates one, importing its
    module lazily so an unavailable backend (e.g. tonio on a GIL build) is only
    touched when actually selected."""

    tonio = "tonio"
    asyncio = "asyncio"

    def create(self) -> Any:
        if self is Backend.tonio:
            from .tonio import TonioBackend

            return TonioBackend()
        from .asyncio import AsyncioBackend

        return AsyncioBackend()


if TYPE_CHECKING:
    from .asyncio import AsyncioBackend
    from .tonio import TonioBackend

    # What every public `backend=` parameter accepts: a `Backend` enum member (the
    # documented path) or an already-created backend instance (`httpunk.asyncio`
    # passes one; `resolve()` returns whatever it's given). The backends share an
    # implicit interface but no nominal base, so this is a union of the concretes.
    BackendLike = Backend | AsyncioBackend | TonioBackend


def resolve(backend) -> Any:
    """Resolve a `backend` argument to a backend instance: a `Backend` member →
    a fresh instance (lazy import); an instance → itself. `None` raises — there is
    no default (see the module docstring). Call this **module-qualified**
    (`_backend.resolve(...)`) so it stays a single monkeypatch point for tests."""
    if backend is None:
        raise ValueError(
            "a backend is required: pass backend=Backend.asyncio or Backend.tonio "
            "(or a backend instance). There is no default — tonio needs free-threaded "
            "CPython >= 3.14, while asyncio is available everywhere."
        )
    if isinstance(backend, Backend):
        return backend.create()
    return backend
