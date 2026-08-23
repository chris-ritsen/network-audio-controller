from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAXIMUM_SOURCE_LINES = 900
SOURCE_SUFFIXES = frozenset({".c", ".h", ".js", ".py", ".rs", ".sh", ".swift", ".ts", ".tsx"})
IGNORED_DIRECTORY_NAMES = frozenset({".git", ".venv", "node_modules", "target"})


def source_line_count(path: Path) -> int:
    content = path.read_bytes()
    return content.count(b"\n") + int(bool(content) and not content.endswith(b"\n"))


def test_source_files_do_not_exceed_nine_hundred_lines():
    oversized_files = []
    for path in PROJECT_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
            continue
        if any(directory_name in IGNORED_DIRECTORY_NAMES for directory_name in path.relative_to(PROJECT_ROOT).parts):
            continue
        line_count = source_line_count(path)
        if line_count > MAXIMUM_SOURCE_LINES:
            oversized_files.append(f"{path.relative_to(PROJECT_ROOT)}: {line_count} lines")

    assert not oversized_files, "Source files exceed the 900-line limit:\n" + "\n".join(sorted(oversized_files))
