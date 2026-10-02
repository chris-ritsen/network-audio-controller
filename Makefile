.PHONY: core header core-types prune-core test test-webapp quality wheel-smoke install restart deploy dev check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes man install-man
.DEFAULT_GOAL := help
PYTHON ?= .venv/bin/python
TEST_CASES ?=
WEB_TESTS ?=
RUST_TEST ?=
LINT_FILES ?=
BOUNDED := python3 scripts/run_bounded_tests.py

.PHONY: help test-python-full test-webapp-full test-full test-rust test-browser lint audit audit-fix
help:
	@echo "test TEST_CASES='tests/test_name.py::test_name'  Focused Python checks; no build or dependency sync"
	@echo "test-webapp WEB_TESTS='tests/webapp/name.test.mjs'  Focused JavaScript checks"
	@echo "test-rust RUST_TEST='test_name'                  Focused Rust checks"
	@echo "test-browser WEB_TESTS='path/to/browser.test.mjs'  Explicit browser UI checks"
	@echo "lint LINT_FILES='path/to/changed.py'             Focused Python lint"
	@echo "test-full / quality                           Explicit broad checks; not routine"
	@echo "check-local                                   Shared local/CI checks with a report; five-minute limit"
	@echo "core / install / deploy                        Explicit build and deployment commands"
	@echo "audit                                         Known vulnerabilities in uv.lock and the core's Cargo.lock"
	@echo "audit-fix                                     Upgrade the vulnerable packages in their lockfiles, then audit"

header:
	cbindgen --config packages/netaudio-core/cbindgen.toml --crate netaudio-core --output packages/netaudio-core/include/netaudio_core.h packages/netaudio-core
	python3 scripts/generate_core_binding.py

core-types:
	$(PYTHON) scripts/generate_core_types.py

CORE_PACKAGE_DIR := packages/netaudio/src/netaudio/core
CORE_DIR := packages/netaudio-core
CORE_RELEASE_DIR := $(CORE_DIR)/target/release

prune-core:
	@cargo sweep --version >/dev/null 2>&1 || cargo install --locked cargo-sweep
	cargo sweep --toolchains "$$(rustup show active-toolchain | cut -d' ' -f1)" $(CORE_DIR)

core: header
	cargo build --release --manifest-path $(CORE_DIR)/Cargo.toml
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

install:
	uv tool install '.[redis]' --force --reinstall-package netaudio

restart:
	launchctl kickstart -k gui/$$(id -u)/com.netaudio.daemon

deploy: install restart

dev:
	@echo "Watching for changes... (restart daemon on *.py save)"
	@find packages/netaudio/src -name '*.py' | entr -r make restart

test:
	@test -n "$(strip $(TEST_CASES))" || { echo "Set TEST_CASES to the affected test file or method; use test-full only for an explicit full run."; exit 2; }
	$(BOUNDED) $(PYTHON) -m pytest -q $(TEST_CASES)

test-webapp:
	@test -n "$(strip $(WEB_TESTS))" || { echo "Set WEB_TESTS to the affected JavaScript test files."; exit 2; }
	$(BOUNDED) node --import ./tests/webapp/loader.mjs --import ./tests/webapp/setup.mjs --test $(WEB_TESTS)

test-rust:
	@test -n "$(strip $(RUST_TEST))" || { echo "Set RUST_TEST to the affected Rust test name."; exit 2; }
	$(BOUNDED) cargo test --locked --offline --manifest-path $(CORE_DIR)/Cargo.toml $(RUST_TEST)

test-browser:
	@test -n "$(strip $(WEB_TESTS))" || { echo "Set WEB_TESTS to the affected browser test files."; exit 2; }
	$(BOUNDED) ./node_modules/.bin/playwright test --project=chromium --workers=1 --retries=0 $(WEB_TESTS)

test-python-full:
	$(PYTHON) -m pytest -q

test-webapp-full:
	node --import ./tests/webapp/loader.mjs --import ./tests/webapp/setup.mjs --test "tests/webapp/*.test.mjs"

test-full: test-webapp-full test-python-full

lint:
	@test -n "$(strip $(LINT_FILES))" || { echo "Set LINT_FILES to the changed Python files; use quality for a full check."; exit 2; }
	$(BOUNDED) $(PYTHON) -m ruff check $(LINT_FILES)

quality:
	$(PYTHON) scripts/check_project.py --scope native --offline

audit:
	$(PYTHON) scripts/audit_dependencies.py

audit-fix:
	$(PYTHON) scripts/audit_dependencies.py --fix

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
	uv run netaudio lab provenance check

check-local:
	$(PYTHON) scripts/check_project.py --scope all --offline

seed-opcode-fixtures:
	uv run netaudio lab provenance seed --clean

label-observed-opcodes:
	uv run netaudio lab provenance label --interactive

man:
	uv run python packages/netaudio/generate_man.py packages/netaudio/man

install-man: man
	install -d $(HOME)/.local/share/man/man1
	install -m644 packages/netaudio/man/*.1 $(HOME)/.local/share/man/man1/
