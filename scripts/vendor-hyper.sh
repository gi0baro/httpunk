#!/usr/bin/env bash
#
# (Re)vendor hyperium/hyper's synchronous, sans-IO HTTP/1 modules into
# crates/vendor-hyper/src/. Run this to bump the vendored version or verify it
# hasn't drifted. See THIRD-PARTY.md.
#
# Usage:
#   scripts/vendor-hyper.sh                    # vendor the pinned version
#   HYPER_VERSION=1.11.0 scripts/vendor-hyper.sh
#
# Like vendor-h2.sh, the copy is kept byte-identical to upstream so `git diff`
# between two vendored versions shows only genuine upstream changes. The only
# modifications are the uniform pub(crate)->pub widening and two documented
# client-only shims (encode.rs / decode.rs; see below and THIRD-PARTY.md).
#
# Built with the `client` + `server` + `http1` features (both roles, HTTP/1).
#
# NOT copied by this script — hand-authored glue + structurally-shimmed module
# files, maintained by hand (re-merge on a version bump; same tradeoff as h2's
# proto/codec mod glue):
#   lib.rs, body/mod.rs, common/mod.rs, proto/h1/io.rs, proto/h1/httpunk.rs  (glue)
#   proto/mod.rs, proto/h1/mod.rs                             (vendored types +
#     structural shims: drop the unvendored conn/dispatch/h2/upgrade wiring)
set -euo pipefail

HYPER_VERSION="${HYPER_VERSION:-1.10.1}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DST="$ROOT/crates/vendor-hyper/src"
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"

find_src() {
  find "$CARGO_HOME/registry/src" -maxdepth 2 -type d -name "hyper-$HYPER_VERSION" 2>/dev/null | head -1
}

SRC="$(find_src)"
if [ -z "$SRC" ]; then
  echo "hyper $HYPER_VERSION not in the cargo cache; fetching…"
  TMP="$(mktemp -d)"
  ( cd "$TMP" && cargo new --lib _hyperfetch -q && cd _hyperfetch \
      && cargo add "hyper@=$HYPER_VERSION" -q && cargo fetch -q )
  rm -rf "$TMP"
  SRC="$(find_src)"
fi
[ -n "$SRC" ] || { echo "error: could not locate hyper-$HYPER_VERSION source" >&2; exit 1; }
echo "vendoring from: $SRC"

# 1. Copy the sans-IO leaf modules verbatim (head parse/encode, the body
#    Encoder + Decoder, and the sync support they pull in). The genuinely-async
#    orchestration (conn.rs, dispatch.rs, most of io.rs, client/server/upgrade)
#    is NOT vendored — that is rewritten in Python.
mkdir -p "$DST/proto/h1" "$DST/body" "$DST/ext" "$DST/common"
cp "$SRC/src/proto/h1/role.rs" "$DST/proto/h1/role.rs"
cp "$SRC/src/proto/h1/encode.rs" "$DST/proto/h1/encode.rs"
cp "$SRC/src/proto/h1/decode.rs" "$DST/proto/h1/decode.rs"
cp "$SRC/src/body/length.rs" "$DST/body/length.rs"
cp "$SRC/src/headers.rs" "$DST/headers.rs"
cp "$SRC/src/error.rs" "$DST/error.rs"
cp "$SRC/src/ext/mod.rs" "$DST/ext/mod.rs"
cp "$SRC/src/ext/informational.rs" "$DST/ext/informational.rs"
cp "$SRC/src/ext/h1_reason_phrase.rs" "$DST/ext/h1_reason_phrase.rs"
cp "$SRC/src/common/date.rs" "$DST/common/date.rs"  # cached Date header (server role)
cp "$SRC/src/cfg.rs" "$DST/cfg.rs"
cp "$SRC/src/trace.rs" "$DST/trace.rs"

# 2. encode.rs shim: drop the sole `WriteBuf`-coupled method,
#    `Encoder::encode_and_end` (the last method of `impl Encoder`), plus its
#    `use super::io::WriteBuf;` — the `io` module is not vendored (its equivalent
#    write path is reimplemented in Python). Everything else is sans-IO.
perl -0pi -e 's/use super::io::WriteBuf;\n//' "$DST/proto/h1/encode.rs"
perl -0pi -e 's/\n\n    pub\(super\) fn encode_and_end<B>.*?\n    \}\n\}\n/\n}\n/s' "$DST/proto/h1/encode.rs"

# 3. decode.rs shims (the decoder is Poll-shaped but pure-sync; driven via the
#    facade's SyncMemRead — see httpunk.rs):
#    a. `use futures_core::ready` -> `use std::task::ready` (avoids a futures-core dep);
#    b. strip the `#[cfg(test)]` `decode_fut` helper and the trailing test module
#       (they use futures_util/tokio dev-deps — like h2's stripped hpack/test).
perl -0pi -e 's/use futures_core::ready;/use std::task::ready;/' "$DST/proto/h1/decode.rs"
perl -0pi -e 's/\n    #\[cfg\(test\)\]\n    async fn decode_fut<R: MemRead>.*?\n    \}\n/\n/s' "$DST/proto/h1/decode.rs"
perl -0pi -e 's/\n+#\[cfg\(test\)\]\nmod tests \{.*\z/\n/s' "$DST/proto/h1/decode.rs"

# 3b. common/date.rs shim: strip the trailing `#[cfg(test)]` test module (like
#     decode.rs). The `#[cfg(feature = "http2")]` bits compile out (http2 off).
perl -0pi -e 's/\n+#\[cfg\(test\)\]\nmod tests \{.*\z/\n/s' "$DST/common/date.rs"

# 4. The ONE byte-parity exception (same as h2): widen `pub(crate)` -> `pub` on
#    the copied files so the main `_httpunk` crate (a separate workspace member)
#    can reach the items its PyO3 adapters + the httpunk facade drive. The glue
#    files listed at the top are not copied here and are left untouched.
find "$DST/proto/h1/role.rs" "$DST/proto/h1/encode.rs" "$DST/proto/h1/decode.rs" \
     "$DST/body/length.rs" "$DST/headers.rs" "$DST/error.rs" \
     "$DST/ext/mod.rs" "$DST/ext/informational.rs" "$DST/ext/h1_reason_phrase.rs" \
     "$DST/common/date.rs" "$DST/cfg.rs" "$DST/trace.rs" -type f -print0 \
  | xargs -0 perl -0pi -e 's/\bpub\(crate\)/pub/g'

# 5. License + version stamp.
cp "$SRC/LICENSE" "$DST/../LICENSE"
echo "$HYPER_VERSION" > "$DST/../UPSTREAM_VERSION"

echo "vendored hyper $HYPER_VERSION into crates/vendor-hyper/src/ (proto/h1/{role,encode,decode} + body/length + headers + error + ext + common/date + cfg + trace)"
