# AGENTS.md

httpunk brings the hyper stack (h2, hyper, non-legacy hyper-util) to Python. It reuses
hyper for its correctness: fidelity to it is the point of the project. The code is the
only source of truth; this file holds rules, not descriptions.

## Fidelity

- Behave 1:1 with upstream at the vendored version (`crates/*/UPSTREAM_VERSION`). A
  divergence is a defect unless the Python runtime forces it or the maintainer decided
  it. This holds for new opt-in APIs too.
- Port from upstream's code, never by analogy or intuition, and cite it as `file.rs:line`.
- Mark every forced or decided divergence at the code with a `DIVERGENCE:` comment
  saying what differs and why.
- hyper-util `client::legacy` is never a reference.
- Keep the h1 and h2 public surfaces parallel.

## Structure

- Pure sans-IO upstream code is vendored byte-identical by `scripts/vendor-*.sh`; the
  scripts are the complete list of modifications. Never hand-edit or reformat vendored
  files.
- Upstream logic that cannot be vendored is adapted: synchronous state in Rust (`src/`),
  async and runtime code in Python (`httpunk/`). The async design is based on tonio and
  then adapted to asyncio; everything runs on both backends.

## Performance

Adapting is not translating. The adapted layers are designed to be as fast as the Python
runtime allows, with behaviour identical to upstream: a wire-visible change is never a
performance option. A performance regression is a defect.

- Tonio is the basis: design and evaluate for tonio first, then for asyncio.
- The cost is in Python: keep hot paths short (fewer frames, awaits and objects per
  request), cross the Python/Rust boundary as rarely as possible and without copies,
  keep synchronous state and logic in Rust, batch writes.
- Measure before claiming: interleaved A/B against the baseline on a release build,
  tonio first, then asyncio, plus cProfile calls per request. Sequential baselines drift.

## Tests

Upstream's tests are the source of truth; existing tests carry no weight against them.

- Vendored code: no tests, upstream tests it.
- Adapted code: tested to match upstream by porting upstream's tests; where upstream has
  none, a derived test is optional, case by case.
- Code in addition to upstream: tests decided case by case.
- Decided divergences: no mirrored tests.

Never reshape production code to make it testable. A race that cannot be reproduced
deterministically gets a `REGRESSION GUARD:` comment at the line instead of a test.

## Errors

Stored exceptions follow store-copies, raise-copies (`fresh_exc`).

## Public API

Breaking changes to the public API (names, signatures, exception types) need the
maintainer's ruling: downstream projects depend on httpunk.

## Docs

No markdown file is a source of truth, and code never references one. Working documents
(plans, notes) are allowed but are throw-away: they are never committed and never cited
from code.
