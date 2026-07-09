.DEFAULT_GOAL := all
pysources = httpunk tests
# The vendored crates (crates/vendor-h2, crates/vendor-hyper) are kept
# byte-identical to upstream (modulo a scripted `pub(crate)`->`pub` widening) and
# must never be reformatted. They're separate workspace members, so `-p httpunk`
# scopes fmt/clippy to just this crate and skips them.
crate = httpunk

.PHONY: build-dev
build-dev:
	@rm -f httpunk/*.so
	uv sync --group all
	uv run maturin develop --uv

.PHONY: format
format:
	uv run ruff check --fix $(pysources)
	uv run ruff format $(pysources)
	cargo fmt -p $(crate)

.PHONY: lint-python
lint-python:
	uv run ruff check $(pysources)
	uv run ruff format --check $(pysources)

.PHONY: lint-rust
lint-rust:
	cargo fmt --version
	cargo fmt -p $(crate) --check
	cargo clippy --version
	cargo clippy -p $(crate) --tests -- \
		-D warnings \
		-W clippy::pedantic \
		-W clippy::dbg_macro \
		-A clippy::cast-possible-truncation \
		-A clippy::cast-sign-loss \
		-A clippy::declare-interior-mutable-const \
		-A clippy::doc-markdown \
		-A clippy::inline-always \
		-A clippy::match-bool \
		-A clippy::match-same-arms \
		-A clippy::module-name-repetitions \
		-A clippy::needless-pass-by-value \
		-A clippy::no-effect-underscore-binding \
		-A clippy::similar-names \
		-A clippy::single-match-else \
		-A clippy::struct-excessive-bools \
		-A clippy::struct-field-names \
		-A clippy::too-many-arguments \
		-A clippy::too-many-lines \
		-A clippy::type-complexity \
		-A clippy::unused-self \
		-A clippy::wrong-self-convention

.PHONY: lint
lint: lint-python lint-rust

.PHONY: vendor-h2
vendor-h2:
	./scripts/vendor-h2.sh

.PHONY: audit
audit:
	cargo audit

.PHONY: test
test:
	uv run pytest -v tests

.PHONY: all
all: format build-dev lint test
