.PHONY: test check-label-provenance check-local seed-opcode-fixtures label-observed-opcodes man install-man

test:
	uv run pytest -q

check-label-provenance:
	uv run netaudio capture provenance check

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
