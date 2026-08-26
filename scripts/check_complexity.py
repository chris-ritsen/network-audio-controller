from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import inspect
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = PROJECT_ROOT / "complexity-baseline.json"
BASELINE_REPOSITORY_PATH = BASELINE_PATH.relative_to(PROJECT_ROOT).as_posix()
MAXIMUM_COMPLEXITY = 15
BASELINE_SCHEMA_VERSION = 1
RUFF_BATCH_SIZE = 100
IGNORED_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "target",
    }
)
MESSAGE_PATTERN = re.compile(r"^`(?P<name>[^`]+)` is too complex \((?P<score>\d+) > (?P<limit>\d+)\)$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ComplexityCheckError(RuntimeError):
    pass


@dataclass(frozen=True)
class ComplexityException:
    complexity: int
    source_sha256: str

    def as_json(self) -> dict[str, int | str]:
        return {
            "complexity": self.complexity,
            "source_sha256": self.source_sha256,
        }


@dataclass(frozen=True)
class _Definition:
    baseline_name: str
    function_name: str
    source_sha256: str


def _definition_fingerprint(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Hash semantic source while assigning nested definitions their own identity."""
    root = copy.deepcopy(node)
    for descendant in ast.walk(root):
        if hasattr(descendant, "type_params"):
            delattr(descendant, "type_params")
        if descendant is root:
            continue
        if isinstance(descendant, (ast.FunctionDef, ast.AsyncFunctionDef)):
            descendant.body = []
    dump_options = {"annotate_fields": True, "include_attributes": False}
    if "show_empty" in inspect.signature(ast.dump).parameters:
        dump_options["show_empty"] = True
    semantic_source = ast.dump(root, **dump_options)
    return hashlib.sha256(semantic_source.encode("utf-8")).hexdigest()


def _accessor_role(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Name) and target.id == "property":
            return "getter"
        if isinstance(target, ast.Attribute) and target.attr in {"getter", "setter", "deleter"}:
            return target.attr
    return None


def _qualified_definitions(source_path: Path) -> dict[int, _Definition]:
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    definitions: dict[int, _Definition] = {}

    def visit(node: ast.AST, parents: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, (*parents, child.name))
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                role = _accessor_role(child)
                component = f"{child.name}[{role}]" if role else child.name
                definition = _Definition(
                    baseline_name=".".join((*parents, component)),
                    function_name=child.name,
                    source_sha256=_definition_fingerprint(child),
                )
                definitions[child.lineno] = definition
                for decorator in child.decorator_list:
                    definitions[decorator.lineno] = definition
                visit(child, (*parents, component))
            else:
                visit(child, parents)

    visit(tree, ())
    return definitions


def _python_source_paths() -> list[Path]:
    paths = []
    for path in PROJECT_ROOT.rglob("*.py"):
        if not path.is_file():
            continue
        relative_path = path.relative_to(PROJECT_ROOT)
        if any(part in IGNORED_DIRECTORY_NAMES for part in relative_path.parts):
            continue
        paths.append(relative_path)
    return sorted(paths)


def _ruff_diagnostics() -> list[dict]:
    ruff = shutil.which("ruff")
    if ruff is None:
        raise ComplexityCheckError("ruff is unavailable; run the checker through `uv run`")
    source_paths = _python_source_paths()
    if not source_paths:
        raise ComplexityCheckError("no Python source files were found")
    diagnostics = []
    for start in range(0, len(source_paths), RUFF_BATCH_SIZE):
        batch = source_paths[start : start + RUFF_BATCH_SIZE]
        completed = subprocess.run(
            [
                ruff,
                "check",
                "--isolated",
                "--select",
                "C901",
                "--config",
                f"lint.mccabe.max-complexity = {MAXIMUM_COMPLEXITY}",
                "--ignore-noqa",
                "--output-format",
                "json",
                *(path.as_posix() for path in batch),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        if completed.returncode not in {0, 1}:
            detail = completed.stderr.strip() or completed.stdout.strip() or f"ruff exited {completed.returncode}"
            raise ComplexityCheckError(detail)
        try:
            batch_diagnostics = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise ComplexityCheckError("ruff returned invalid JSON") from error
        if not isinstance(batch_diagnostics, list):
            raise ComplexityCheckError("ruff returned an invalid diagnostic collection")
        diagnostics.extend(batch_diagnostics)
    return diagnostics


def current_exceptions() -> dict[str, ComplexityException]:
    definitions_by_path: dict[Path, dict[int, _Definition]] = {}
    exceptions: dict[str, ComplexityException] = {}
    for diagnostic in _ruff_diagnostics():
        message = diagnostic.get("message")
        match = MESSAGE_PATTERN.fullmatch(message) if isinstance(message, str) else None
        if match is None:
            raise ComplexityCheckError(f"unexpected Ruff C901 message: {message!r}")
        reported_limit = int(match.group("limit"))
        if reported_limit != MAXIMUM_COMPLEXITY:
            raise ComplexityCheckError(
                f"Ruff reported complexity limit {reported_limit}; expected {MAXIMUM_COMPLEXITY}"
            )
        source_path = Path(diagnostic["filename"]).resolve()
        try:
            relative_path = source_path.relative_to(PROJECT_ROOT).as_posix()
        except ValueError as error:
            raise ComplexityCheckError(f"Ruff reported a path outside the repository: {source_path}") from error
        definitions = definitions_by_path.setdefault(source_path, _qualified_definitions(source_path))
        line = diagnostic.get("location", {}).get("row")
        definition = definitions.get(line)
        if definition is None:
            raise ComplexityCheckError(f"could not resolve the function at {relative_path}:{line}")
        reported_name = match.group("name")
        if definition.function_name != reported_name:
            raise ComplexityCheckError(
                f"Ruff reported {reported_name!r} at {relative_path}:{line}, resolved as {definition.baseline_name!r}"
            )
        key = f"{relative_path}::{definition.baseline_name}"
        if key in exceptions:
            raise ComplexityCheckError(f"duplicate complexity exception key: {key}")
        exceptions[key] = ComplexityException(
            complexity=int(match.group("score")),
            source_sha256=definition.source_sha256,
        )
    return dict(sorted(exceptions.items()))


def _parse_baseline(text: str, source: str) -> dict[str, ComplexityException]:
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise ComplexityCheckError(f"{source} is not valid JSON") from error
    if not isinstance(document, dict) or document.get("schema_version") != BASELINE_SCHEMA_VERSION:
        raise ComplexityCheckError(f"{source} uses the wrong schema version")
    if document.get("maximum_complexity") != MAXIMUM_COMPLEXITY:
        raise ComplexityCheckError(f"{source} uses the wrong maximum")
    raw_exceptions = document.get("exceptions")
    if not isinstance(raw_exceptions, dict):
        raise ComplexityCheckError(f"{source} exceptions are invalid")

    exceptions = {}
    for key, value in raw_exceptions.items():
        if not isinstance(key, str) or not key or not isinstance(value, dict):
            raise ComplexityCheckError(f"{source} exceptions are invalid")
        if set(value) != {"complexity", "source_sha256"}:
            raise ComplexityCheckError(f"{source} exception {key!r} has invalid fields")
        complexity = value["complexity"]
        source_sha256 = value["source_sha256"]
        if (
            isinstance(complexity, bool)
            or not isinstance(complexity, int)
            or complexity <= MAXIMUM_COMPLEXITY
            or not isinstance(source_sha256, str)
            or SHA256_PATTERN.fullmatch(source_sha256) is None
        ):
            raise ComplexityCheckError(f"{source} exception {key!r} is invalid")
        exceptions[key] = ComplexityException(complexity, source_sha256)
    return dict(sorted(exceptions.items()))


def _load_baseline() -> dict[str, ComplexityException]:
    if not BASELINE_PATH.exists():
        raise ComplexityCheckError(
            f"complexity baseline is missing; initialize it with "
            f"`{sys.executable} {Path(__file__).name} --update-baseline`"
        )
    try:
        text = BASELINE_PATH.read_text(encoding="utf-8")
    except OSError as error:
        raise ComplexityCheckError("complexity baseline is unreadable") from error
    return _parse_baseline(text, BASELINE_REPOSITORY_PATH)


def _git_output(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def _baseline_at_revision(revision: str) -> dict[str, ComplexityException] | None:
    resolved = _git_output("rev-parse", "--verify", f"{revision}^{{commit}}")
    if resolved.returncode != 0:
        raise ComplexityCheckError(f"could not resolve Git revision {revision!r}")
    tree = _git_output("ls-tree", "-r", "--name-only", revision, "--", BASELINE_REPOSITORY_PATH)
    if tree.returncode != 0:
        raise ComplexityCheckError(f"could not inspect {BASELINE_REPOSITORY_PATH} at {revision}")
    if BASELINE_REPOSITORY_PATH not in tree.stdout.splitlines():
        return None
    completed = _git_output("show", f"{revision}:{BASELINE_REPOSITORY_PATH}")
    if completed.returncode != 0:
        raise ComplexityCheckError(f"could not read {BASELINE_REPOSITORY_PATH} at {revision}")
    return _parse_baseline(completed.stdout, f"{revision}:{BASELINE_REPOSITORY_PATH}")


def _historical_baselines(revision: str) -> list[tuple[str, dict[str, ComplexityException]]]:
    history = _git_output("log", "--first-parent", "--format=%H", revision, "--", BASELINE_REPOSITORY_PATH)
    if history.returncode != 0:
        raise ComplexityCheckError(f"could not inspect complexity baseline history from {revision}")
    baselines = []
    for commit in history.stdout.splitlines():
        baseline = _baseline_at_revision(commit)
        if baseline is not None:
            baselines.append((commit, baseline))
    return baselines


def _reference_baselines(
    candidate: dict[str, ComplexityException],
) -> list[tuple[str, dict[str, ComplexityException]]]:
    inside_worktree = _git_output("rev-parse", "--is-inside-work-tree")
    if inside_worktree.returncode != 0 or inside_worktree.stdout.strip() != "true":
        raise ComplexityCheckError("could not verify the Git worktree for the complexity baseline")
    shallow = _git_output("rev-parse", "--is-shallow-repository")
    if shallow.returncode != 0:
        raise ComplexityCheckError("could not determine whether Git history is shallow")
    if shallow.stdout.strip() == "true":
        raise ComplexityCheckError("Git history is too shallow to verify the reduction-only baseline")

    head = _baseline_at_revision("HEAD")
    if head is not None and head != candidate:
        history_start = "HEAD"
    elif head is None:
        history_start = "HEAD"
    else:
        parents = _git_output("rev-list", "--parents", "-n", "1", "HEAD")
        if parents.returncode != 0:
            raise ComplexityCheckError("could not inspect the parent of HEAD")
        revisions = parents.stdout.split()
        if len(revisions) < 2:
            return []
        history_start = revisions[1]

    return _historical_baselines(history_start)


def _transition_differences(
    previous: dict[str, ComplexityException],
    candidate: dict[str, ComplexityException],
) -> list[str]:
    differences = []
    for key in sorted(candidate.keys() - previous.keys()):
        differences.append(f"baseline allowance was added: {key} = {candidate[key].complexity}")
    for key in sorted(candidate.keys() & previous.keys()):
        old = previous[key]
        new = candidate[key]
        if new.complexity > old.complexity:
            differences.append(f"baseline allowance was raised: {key} = {new.complexity} (previous {old.complexity})")
        elif new.complexity == old.complexity and new.source_sha256 != old.source_sha256:
            differences.append(f"baseline fingerprint changed without a complexity reduction: {key}")
    return differences


def _history_transition_differences(
    candidate: dict[str, ComplexityException],
    history: list[tuple[str, dict[str, ComplexityException]]],
) -> list[str]:
    differences = []
    newer = candidate
    newer_label = "current baseline"
    for commit, older in history:
        transition = _transition_differences(older, newer)
        differences.extend(f"{newer_label} relative to {commit[:12]}: {difference}" for difference in transition)
        newer = older
        newer_label = commit[:12]
    return differences


def _verify_reduction_only_history(candidate: dict[str, ComplexityException]) -> None:
    differences = _history_transition_differences(candidate, _reference_baselines(candidate))
    if differences:
        raise ComplexityCheckError(
            "complexity baseline is not reduction-only relative to Git history:\n"
            + "\n".join(f"- {difference}" for difference in differences)
        )


def _source_differences(
    baseline: dict[str, ComplexityException],
    current: dict[str, ComplexityException],
) -> list[str]:
    differences = []
    for key in sorted(current.keys() - baseline.keys()):
        differences.append(f"new over-limit callable: {key} = {current[key].complexity}")
    for key in sorted(baseline.keys() - current.keys()):
        differences.append(f"stale baseline allowance: {key} = {baseline[key].complexity}")
    for key in sorted(current.keys() & baseline.keys()):
        expected = baseline[key]
        observed = current[key]
        if observed.complexity > expected.complexity:
            differences.append(f"complexity increased: {key} = {observed.complexity} (baseline {expected.complexity})")
        elif observed.complexity < expected.complexity:
            differences.append(
                f"baseline can be lowered: {key} = {observed.complexity} (baseline {expected.complexity})"
            )
        elif observed.source_sha256 != expected.source_sha256:
            differences.append(f"legacy callable changed without reducing complexity: {key}")
    return differences


def check_baseline() -> None:
    baseline = _load_baseline()
    _verify_reduction_only_history(baseline)
    current = current_exceptions()
    differences = _source_differences(baseline, current)
    if differences:
        raise ComplexityCheckError(
            "complexity baseline does not match the source:\n"
            + "\n".join(f"- {difference}" for difference in differences)
            + "\nRun with --update-baseline after reducing existing complexity."
        )


def update_baseline() -> None:
    current = current_exceptions()
    if BASELINE_PATH.exists():
        baseline = _load_baseline()
        forbidden = _transition_differences(baseline, current)
        if forbidden:
            raise ComplexityCheckError("complexity baseline is reduction-only:\n" + "\n".join(forbidden))
    _verify_reduction_only_history(current)
    document = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "maximum_complexity": MAXIMUM_COMPLEXITY,
        "exceptions": {key: value.as_json() for key, value in current.items()},
    }
    BASELINE_PATH.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {len(current)} legacy exceptions to {BASELINE_PATH.relative_to(PROJECT_ROOT)}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--update-baseline", action="store_true")
    arguments = parser.parse_args()
    try:
        if arguments.update_baseline:
            update_baseline()
        else:
            check_baseline()
    except (ComplexityCheckError, OSError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
