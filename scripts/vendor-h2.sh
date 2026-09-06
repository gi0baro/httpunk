#!/usr/bin/env bash
#
# (Re)vendor hyperium/h2's synchronous, sans-IO modules (frame + hpack + ext)
# into crates/vendor-h2/src/. Run this to bump the vendored version or verify it hasn't
# drifted. See THIRD-PARTY.md.
#
# Usage:
#   scripts/vendor-h2.sh                 # vendor the pinned version
#   H2_VERSION=0.4.19 scripts/vendor-h2.sh
#
# The copy is kept byte-identical to upstream so `git diff` between two vendored
# versions shows only genuine upstream changes. Modifications: drop hpack/test/, one documented state.rs shim, and a
# uniform pub(crate)->pub widening (see steps below and THIRD-PARTY.md).
set -euo pipefail

H2_VERSION="${H2_VERSION:-0.4.19}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DST="$ROOT/crates/vendor-h2/src"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"

find_src() {
  find "$CARGO_HOME/registry/src" -maxdepth 2 -type d -name "h2-$H2_VERSION" 2>/dev/null | head -1
}

SRC="$(find_src)"
if [ -z "$SRC" ]; then
  echo "h2 $H2_VERSION not in the cargo cache; fetching…"
  TMP="$(mktemp -d)"
  ( cd "$TMP" && cargo new --lib _h2fetch -q && cd _h2fetch \
      && cargo add "h2@=$H2_VERSION" -q && cargo fetch -q )
  rm -rf "$TMP"
  SRC="$(find_src)"
fi
[ -n "$SRC" ] || { echo "error: could not locate h2-$H2_VERSION source" >&2; exit 1; }
echo "vendoring from: $SRC"

# 1. Copy the sans-IO modules verbatim.
rm -rf "$DST/frame" "$DST/hpack" "$DST/ext.rs"
cp -R "$SRC/src/frame" "$DST/frame"
cp -R "$SRC/src/hpack" "$DST/hpack"
cp "$SRC/src/ext.rs" "$DST/ext.rs"

# 2. Sole modification: drop the hpack test fixtures (they pull external
#    test-only deps) and the `#[cfg(test)] mod test;` line that references them.
#    Inline `#[cfg(test)] mod tests {…}` blocks are std-only and kept verbatim.
rm -rf "$DST/hpack/test"
perl -0pi -e 's/#\[cfg\(test\)\]\s*\nmod test;\n//' "$DST/hpack/mod.rs"

# 3. Vendor the *synchronous* proto pieces: the stream state machine + flow
#    control + the error types they reference. The async proto/streams
#    orchestration (recv/send/prioritize/streams/store/buffer/counts) and
#    connection/ping_pong/go_away are NOT vendored — their state is re-expressed in
#    src/h2/conn.rs (H2Streams) and their async half in Python.
#    The hand-written module glue (crates/vendor-h2/src/{lib,proto,codec}/… glue) is NOT touched by this script.
mkdir -p "$DST/proto/streams" "$DST/codec"
cp "$SRC/src/proto/streams/state.rs" "$DST/proto/streams/state.rs"
cp "$SRC/src/proto/streams/flow_control.rs" "$DST/proto/streams/flow_control.rs"
cp "$SRC/src/proto/error.rs" "$DST/proto/error.rs"
cp "$SRC/src/codec/error.rs" "$DST/codec/error.rs"
#    h2's public `Error` (src/error.rs): pure, and what `State::ensure_reason` returns
#    — vendored so that method stays byte-identical (the connection state derives a
#    stream's stop reason from it; httpunk's own `H2Streams` is the caller).
cp "$SRC/src/error.rs" "$DST/error.rs"

# state.rs modifications (the driver has no `frame::Headers` at open time — the
# decoder already turned it into the Python frame event):
#   b. shim recv_open(&frame::Headers) -> recv_open(eos, informational)
#   (`PollReset` — h2 send.rs L43-48 — lives in the hand-written proto/mod.rs glue,
#   and `crate::Error` is vendored above, so `ensure_reason` is byte-identical.)
perl -0pi -e 's/pub fn recv_open\(&mut self, frame: &frame::Headers\) -> Result<bool, Error> \{\n        let mut initial = false;\n        let eos = frame\.is_end_stream\(\);/pub fn recv_open(\&mut self, eos: bool, informational: bool) -> Result<bool, Error> {\n        let mut initial = false;/' "$DST/proto/streams/state.rs"
perl -0pi -e 's/frame\.is_informational\(\)/informational/g' "$DST/proto/streams/state.rs"
#   d. strip the trailing inline `#[cfg(test)] mod tests` (added in 0.4.16): it
#      drives `recv_open(&frame::Headers)`, the pre-shim signature from (b), so it
#      can't compile against the shimmed file (same tradeoff as hyper's stripped
#      decode.rs/date.rs test modules).
perl -0pi -e 's/\n+#\[cfg\(test\)\]\nmod tests \{.*\z/\n/s' "$DST/proto/streams/state.rs"

# 4. The ONE byte-parity exception: widen `pub(crate)` -> `pub` on the vendored
#    files so the main `_httpunk` crate (a separate workspace member) can reach
#    the h2 items it drives (frame load/encode, hpack, `BytesStr::as_str`, …).
#    Deterministic + idempotent, so a `git diff` between two vendored versions
#    still shows only genuine upstream changes. The hand-written glue
#    (crates/vendor-h2/src/**/mod.rs) is not copied by this script and is left
#    untouched. (This is a separate crate, so vendored files keep upstream's own
#    `crate::` paths verbatim — no namespace rewrite.)
find "$DST/frame" "$DST/hpack" -name '*.rs' -print0 \
  | xargs -0 perl -0pi -e 's/\bpub\(crate\)/pub/g'
perl -0pi -e 's/\bpub\(crate\)/pub/g' \
  "$DST/ext.rs" "$DST/error.rs" \
  "$DST/proto/streams/state.rs" "$DST/proto/streams/flow_control.rs" \
  "$DST/proto/error.rs" "$DST/codec/error.rs"
#    `State::ensure_reason` is `pub(super)` upstream (only `streams::send` calls it);
#    the same widening, so the main crate's `H2Streams` can.
perl -0pi -e 's/\bpub\(super\)/pub/g' "$DST/proto/streams/state.rs"

# 5. License + version stamp.
cp "$SRC/LICENSE" "$DST/../LICENSE"
echo "$H2_VERSION" > "$DST/../UPSTREAM_VERSION"

echo "vendored h2 $H2_VERSION into crates/vendor-h2/src/ (frame + hpack + ext + error + proto{state,flow_control,error} + codec/error)"
