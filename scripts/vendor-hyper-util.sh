#!/usr/bin/env bash
#
# (Re)vendor hyperium/hyper-util's synchronous, sans-IO proxy matcher into
# crates/vendor-hyper-util/src/. Run this to bump the vendored version or verify
# it hasn't drifted. See THIRD-PARTY.md.
#
# Usage:
#   scripts/vendor-hyper-util.sh                       # vendor the pinned version
#   HYPER_UTIL_VERSION=0.1.21 scripts/vendor-hyper-util.sh
#
# Like vendor-h2.sh / vendor-hyper.sh, the copy is kept byte-identical to upstream
# so `git diff` between two vendored versions shows only genuine upstream changes.
# The only modification is the uniform pub(crate)->pub widening (a no-op today —
# matcher.rs has none — kept for parity with the other vendor scripts).
#
# `client::proxy::matcher` is pure, IO-free selection logic, so — unlike graceful /
# connect / auto, which need the async runtime and live in Python — it is vendored
# and exposed to Python via PyO3 (src/py/proxy.rs), same as the h1/h2 codecs.
#
# NOT copied by this script — hand-authored module glue (re-created on a version
# bump): lib.rs, client/mod.rs, client/proxy/mod.rs (minimal wiring that exposes
# only `client::proxy::matcher`, dropping the unvendored legacy/pool/connect etc.).
#
# The vendored matcher.rs keeps its `#[cfg(feature = "client-proxy-system")]`
# `mac`/`win` modules verbatim; that feature stays OFF, so the platform code (and
# its system-configuration/windows-sys deps) is never compiled.
set -euo pipefail

HYPER_UTIL_VERSION="${HYPER_UTIL_VERSION:-0.1.20}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DST="$ROOT/crates/vendor-hyper-util/src"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"

find_src() {
  find "$CARGO_HOME/registry/src" -maxdepth 2 -type d -name "hyper-util-$HYPER_UTIL_VERSION" 2>/dev/null | head -1
}

SRC="$(find_src)"
if [ -z "$SRC" ]; then
  echo "hyper-util $HYPER_UTIL_VERSION not in the cargo cache; fetching…"
  TMP="$(mktemp -d)"
  ( cd "$TMP" && cargo new --lib _hyperutilfetch -q && cd _hyperutilfetch \
      && cargo add "hyper-util@=$HYPER_UTIL_VERSION" -q && cargo fetch -q )
  rm -rf "$TMP"
  SRC="$(find_src)"
fi
[ -n "$SRC" ] || { echo "error: could not locate hyper-util-$HYPER_UTIL_VERSION source" >&2; exit 1; }
echo "vendoring from: $SRC"

# 1. Copy the one sans-IO leaf module verbatim.
mkdir -p "$DST/client/proxy"
cp "$SRC/src/client/proxy/matcher.rs" "$DST/client/proxy/matcher.rs"

# 2. The ONE byte-parity exception (same as h2/hyper): uniform pub(crate)->pub
#    widening so the main `_httpunk` crate's PyO3 adapter can reach the items it
#    drives. (matcher.rs has no pub(crate) today; kept for parity + future bumps.)
perl -0pi -e 's/\bpub\(crate\)/pub/g' "$DST/client/proxy/matcher.rs"

# 3. License + version stamp.
cp "$SRC/LICENSE" "$DST/../LICENSE"
echo "$HYPER_UTIL_VERSION" > "$DST/../UPSTREAM_VERSION"

echo "vendored hyper-util $HYPER_UTIL_VERSION into crates/vendor-hyper-util/src/ (client/proxy/matcher)"
