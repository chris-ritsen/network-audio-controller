# NetAudio agent instructions

Read and follow `AGENTS.md`; it is the canonical project policy. Task handoffs
may add context or narrow a task but may not weaken that policy.

NetAudio is primarily an interoperability reverse-engineering and protocol
research project for Dante Controller and related software. It also ships a
library, CLI, daemon/server components, and future user interfaces. Research
tools, bounded experiments, findings, provenance records, and minimal evidence
fixtures are legitimate repository work, but shipping behavior must not depend
on proprietary applications or lab state at runtime.

Do not join the words `clean` and `room`—with a space, hyphen, underscore, or
no separator—in committed filenames or contents. Use neutral terms such as
`protocol research`, `evidence boundary`, `capture-derived`, or `independent
implementation`. Do not imply that excluded material belongs to a named
parallel process; identify excluded evidence directly.

Permitted evidence includes public material, packet captures from real or
synthetic devices, official tools used through normal user-visible interfaces,
controlled black-box experiments, and authorized runtime observation. Do not
statically inspect proprietary firmware or applications, use recovered source
or internal symbol catalogs, or transfer unsupported proprietary terminology
into the product. Preserve source, run/frame or timestamp, scope, digest, and
allowed/excluded evidence when promoting research artifacts.

Before editing, inspect Git status and preserve every existing dirty or
untracked path. Stage exact paths only. Never use `git checkout`, `git restore`,
`git reset`, or an equivalent overwrite without explicit user confirmation
after showing the exact command and target. Do not commit, rewrite history,
push, or change remotes unless requested.

Keep hand-written source and test files at 900 lines or fewer. New or changed
hand-written Python functions and methods have a hard Ruff `C901` complexity
limit of 15. The checked legacy baseline is reduction-only: do not add or
increase an allowance, hand-edit the baseline, or suppress `C901`. Run
`uv run python scripts/check_complexity.py` with Python changes, use focused
tests first, and keep ordinary tests offline and deterministic.
