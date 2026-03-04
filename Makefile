.PHONY: test check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes

test:
	uv run pytest -q

check-label-provenance:
	uv run netaudio capture provenance check

check-local: check-label-provenance test

seed-opcode-fixtures:
	uv run netaudio capture provenance seed --clean

label-observed-opcodes:
	uv run netaudio capture provenance label --interactive
