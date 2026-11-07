.PHONY: core header core-types prune-core quality wheel-smoke install restart deploy dev check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes man install-man
.DEFAULT_GOAL := help
PYTHON ?= .venv/bin/python
WEB_TESTS ?=
LINT_FILES ?=
BOUNDED := python3 scripts/run_bounded_tests.py

.PHONY: help test-browser test-lab lint audit audit-fix release release-status
help:
	@echo "test-browser WEB_TESTS='path/to/browser.test.mjs'  Explicit browser UI checks"
	@echo "test-lab LAB=path/to/lab.toml [LAB_REPORT=report.xml]  Regression checks against lab devices"
	@echo "lint LINT_FILES='path/to/changed.py'             Focused Python lint"
	@echo "quality                                         Explicit broad checks; not routine"
	@echo "check-local                                   Shared local/CI checks with a report; five-minute limit"
	@echo "core / install / deploy                        Explicit build and deployment commands"
	@echo "audit                                         Known vulnerabilities in uv.lock and the core's Cargo.lock"
	@echo "audit-fix                                     Upgrade the vulnerable packages in their lockfiles, then audit"
	@echo "release [VERSION=x.y.z]                       Background release: checks, tag, GitHub release workflow, PyPI"
	@echo "release-status                                Progress or result of the latest release"

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
	@if [ "$$(uname)" = Darwin ]; then \
		launchctl kickstart -k gui/$$(id -u)/com.netaudio.daemon; \
	else \
		systemctl --user restart netaudio; \
	fi

deploy: install restart

dev:
	@echo "Watching for changes... (restart daemon on *.py save)"
	@find packages/netaudio/src -name '*.py' | entr -r make restart

test-lab:
	@test -n "$(strip $(LAB))" || { echo "Set LAB to a lab file naming the devices to test."; exit 2; }
	NETAUDIO_LAB="$(LAB)" $(PYTHON) -m pytest -q -p no:cacheprovider tests/lab $(if $(LAB_REPORT),--junitxml="$(LAB_REPORT)")

test-browser:
	@test -n "$(strip $(WEB_TESTS))" || { echo "Set WEB_TESTS to the affected browser test files."; exit 2; }
	$(BOUNDED) ./node_modules/.bin/playwright test --project=chromium --workers=1 --retries=0 $(WEB_TESTS)

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

release:
	@$(PYTHON) scripts/release.py $(if $(VERSION),--version $(VERSION))

release-status:
	@$(PYTHON) scripts/release.py --status

seed-opcode-fixtures:
	uv run netaudio lab provenance seed --clean

label-observed-opcodes:
	uv run netaudio lab provenance label --interactive

man:
	uv run python packages/netaudio/generate_man.py packages/netaudio/man

install-man: man
	install -d $(HOME)/.local/share/man/man1
	install -m644 packages/netaudio/man/*.1 $(HOME)/.local/share/man/man1/
