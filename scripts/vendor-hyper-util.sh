#!/usr/bin/env bash
#
# (Re)vendor hyperium/hyper-util's synchronous, sans-IO proxy matcher into
# crates/vendor-hyper-util/src/. Run this to bump the vendored version or verify
# it hasn't drifted. The steps below enumerate every modification.
#
# Usage:
#   scripts/vendor-hyper-util.sh                       # vendor the pinned version
#   HYPER_UTIL_VERSION=0.1.21 scripts/vendor-hyper-util.sh
#
# Like vendor-h2.sh / vendor-hyper.sh, the copy is kept byte-identical to upstream
# so `git diff` between two vendored versions shows only genuine upstream changes.
# The only modification is the uniform pub(crate)->pub widening (a no-op today —
# the copied files have none — kept for parity with the other vendor scripts).
#
# `client::proxy::matcher` is pure, IO-free selection logic, so — unlike graceful /
# connect / auto, which need the async runtime and live in Python — it is vendored
# and exposed to Python via PyO3 (src/proxy.rs), same as the h1/h2 codecs.
#
# NOT copied by this script: lib.rs (hand-authored crate-root glue). Upstream's
# client/mod.rs and client/proxy/mod.rs are copied; their feature gates leave the
# unvendored legacy/pool/connect code off.
#
# The vendored matcher.rs keeps its `#[cfg(feature = "client-proxy-system")]`
# `mac`/`win` modules verbatim; that feature stays OFF, so the platform code (and
# its system-configuration/windows-sys deps) is never compiled.
set -euo pipefail

HYPER_UTIL_VERSION="${HYPER_UTIL_VERSION:-0.1.21}"
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

# 1. Copy the sans-IO matcher and feature-gated module wiring verbatim.
mkdir -p "$DST/client/proxy"
cp "$SRC/src/client/mod.rs" "$DST/client/mod.rs"
cp "$SRC/src/client/proxy/mod.rs" "$DST/client/proxy/mod.rs"
cp "$SRC/src/client/proxy/matcher.rs" "$DST/client/proxy/matcher.rs"

# 2. The ONE byte-parity exception (same as h2/hyper): uniform pub(crate)->pub
#    widening so the main `_httpunk` crate's PyO3 adapter can reach the items it
#    drives. (The copied files have no pub(crate) today; kept for parity.)
perl -0pi -e 's/\bpub\(crate\)/pub/g' \
  "$DST/client/mod.rs" "$DST/client/proxy/mod.rs" "$DST/client/proxy/matcher.rs"

# 3. License + version stamp.
cp "$SRC/LICENSE" "$DST/../LICENSE"
echo "$HYPER_UTIL_VERSION" > "$DST/../UPSTREAM_VERSION"

echo "vendored hyper-util $HYPER_UTIL_VERSION into crates/vendor-hyper-util/src/ (client + client/proxy + client/proxy/matcher)"
