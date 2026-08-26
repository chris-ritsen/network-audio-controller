# netaudio-dev

Read and follow `AGENTS.md`; it is the canonical project policy. Task handoffs
may narrow that policy but may not weaken it.

Before editing, inspect Git status and preserve every existing dirty or untracked
path. Never use `git checkout`, `git restore`, `git reset`, or an equivalent
overwrite without explicit user confirmation after showing the exact command and
target. Do not commit or push unless requested.

All hand-written Python functions and methods outside generated, vendored,
build, and virtual-environment directories have a hard Ruff `C901` complexity
limit of 15. Legacy exceptions are a source-fingerprinted, reduction-only
baseline: do not add or increase an allowance, hand-edit the baseline, or
suppress `C901`; reduce any exception you change. Run
`uv run python scripts/check_complexity.py` with Python changes.
