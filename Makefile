.PHONY: core header example example-swift test quality wheel-smoke install restart deploy dev check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes man install-man site-check site-preview site-publish

header:
	cbindgen --config packages/netaudio-core/cbindgen.toml --crate netaudio-core --output packages/netaudio-core/include/netaudio_core.h packages/netaudio-core

CORE_PACKAGE_DIR := packages/netaudio/src/netaudio/core
CORE_RELEASE_DIR := packages/netaudio-core/target/release

core: header
	cargo build --release --manifest-path packages/netaudio-core/Cargo.toml
	@if [ -f $(CORE_RELEASE_DIR)/libnetaudio_core.so ]; then \
		cp -f $(CORE_RELEASE_DIR)/libnetaudio_core.so $(CORE_PACKAGE_DIR)/libnetaudio_core.so; \
	elif [ -f $(CORE_RELEASE_DIR)/libnetaudio_core.dylib ]; then \
		cp -f $(CORE_RELEASE_DIR)/libnetaudio_core.dylib $(CORE_PACKAGE_DIR)/libnetaudio_core.dylib; \
	elif [ -f $(CORE_RELEASE_DIR)/netaudio_core.dll ]; then \
		cp -f $(CORE_RELEASE_DIR)/netaudio_core.dll $(CORE_PACKAGE_DIR)/netaudio_core.dll; \
	else \
		echo "netaudio-core library missing after cargo build" >&2; \
		exit 1; \
	fi

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
		set -eu; \
		trap 'rm -rf "$$tmp"' 0; \
		uv build --wheel --out-dir "$$tmp"; \
		set -- "$$tmp"/netaudio-*.whl; \
		if [ "$$#" -ne 1 ] || [ ! -f "$$1" ]; then \
			echo "expected exactly one wheel, found $$#" >&2; \
			exit 1; \
		fi; \
		case "$$1" in \
			*-manylinux_2_28_*.whl|*-macosx_11_0_*.whl|*-win_amd64.whl) \
				uv run --isolated --no-project python scripts/verify_wheel_artifact.py "$$1" ;; \
			*) \
				echo "local wheel tag is not a release-policy tag; CI verifies release artifacts" ;; \
		esac; \
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

site-check:
	python3 website/validate.py
	uv run pytest -q website/tests

site-preview:
	python3 -m http.server 8765 --directory website/public

site-publish: site-check
	sudo /usr/bin/python3 website/publish.py --source website/public
