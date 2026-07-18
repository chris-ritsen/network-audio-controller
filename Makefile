.PHONY: core header example example-swift test quality wheel-smoke install restart deploy dev check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes man install-man

header:
	cbindgen --config packages/netaudio-core/cbindgen.toml --crate netaudio-core --output packages/netaudio-core/include/netaudio_core.h packages/netaudio-core

core: header
	cargo build --release --manifest-path packages/netaudio-core/Cargo.toml

example: core
	gcc -o packages/netaudio-core/examples/rename_channel packages/netaudio-core/examples/rename_channel.c \
		-Ipackages/netaudio-core/include -Lpackages/netaudio-core/target/release -lnetaudio_core

example-swift: core
	swiftc -O -import-objc-header packages/netaudio-core/include/netaudio_core.h \
		packages/netaudio-core/examples/rename_channel.swift \
		-Lpackages/netaudio-core/target/release -lnetaudio_core \
		-o packages/netaudio-core/examples/rename_channel_swift

install: core
	uv tool install netaudio --from . --force --no-cache

restart:
	launchctl kickstart -k gui/$$(id -u)/com.netaudio.daemon

deploy: install restart

dev:
	@echo "Watching for changes... (restart daemon on *.py save)"
	@find packages/netaudio/src -name '*.py' | entr -r make restart

test:
	uv run pytest -q

quality:
	uv lock --check
	uv run ruff check .
	uv run ruff format --check .
	uv run pyright
	cargo fmt --manifest-path packages/netaudio-core/Cargo.toml -- --check
	cargo clippy --manifest-path packages/netaudio-core/Cargo.toml --all-targets -- -D warnings
	cargo test --manifest-path packages/netaudio-core/Cargo.toml

wheel-smoke:
	@tmp=$$(mktemp -d) || exit 1; \
		trap 'rm -rf "$$tmp"' 0; \
		uv build --wheel --out-dir "$$tmp"; \
		set -- "$$tmp"/netaudio-*.whl; \
		if [ "$$#" -ne 1 ] || [ ! -f "$$1" ]; then \
			echo "expected exactly one wheel, found $$#" >&2; \
			exit 1; \
		fi; \
		uv run --isolated --no-project python scripts/verify_wheel_artifact.py "$$1"; \
		uv run --isolated --no-project python scripts/smoke_wheel_install.py "$$1"

check-label-provenance:
	uv run netaudio provenance check

check-local: check-label-provenance test

seed-opcode-fixtures:
	uv run netaudio capture provenance seed --clean

label-observed-opcodes:
	uv run netaudio capture provenance label --interactive

man:
	uv run python packages/netaudio/generate_man.py packages/netaudio/man

install-man: man
	install -d $(HOME)/.local/share/man/man1
	install -m644 packages/netaudio/man/*.1 $(HOME)/.local/share/man/man1/
