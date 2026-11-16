from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib


ROOT = Path(__file__).resolve().parent.parent
OSV_QUERY_BATCH = "https://api.osv.dev/v1/querybatch"
OSV_VULNERABILITY = "https://api.osv.dev/v1/vulns/{}"


@dataclass(frozen=True)
class Acceptance:
    identifiers: frozenset[str]
    reason: str


ACCEPTED: dict[tuple[str, str, str], Acceptance] = {}


@dataclass
class Finding:
    ecosystem: str
    package: str
    version: str
    identifiers: set[str] = field(default_factory=set)
    fixed_versions: set[str] = field(default_factory=set)
    summaries: set[str] = field(default_factory=set)

    def acceptance(self) -> Acceptance | None:
        acceptance = ACCEPTED.get((self.ecosystem, self.package, self.version))
        if acceptance is None or not self.identifiers <= acceptance.identifiers:
            return None
        return acceptance


@dataclass(frozen=True)
class Notice:
    package: str
    version: str
    identifier: str
    summary: str


def _python_findings(root: Path) -> list[Finding]:
    result = subprocess.run(
        [
            "uv",
            "audit",
            "--frozen",
            "--no-dev",
            "--preview-features",
            "audit-command",
            "--output-format",
            "json",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"uv audit failed: {result.stderr.strip() or error}") from error
    findings: dict[tuple[str, str], Finding] = {}
    for vulnerability in report.get("vulnerabilities", []):
        dependency = vulnerability["dependency"]
        key = (dependency["name"], dependency["version"])
        finding = findings.setdefault(key, Finding("Python", *key))
        finding.identifiers.add(vulnerability.get("display_id") or vulnerability["id"])
        finding.fixed_versions.update(vulnerability.get("fix_versions") or [])
        summary = vulnerability.get("summary") or vulnerability.get("description") or ""
        if summary:
            finding.summaries.add(summary.strip().splitlines()[0])
    return list(findings.values())


def _osv_request(url: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _rust_findings(root: Path) -> tuple[list[Finding], list[Notice]]:
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    packages = [package for package in lock["package"] if package.get("source", "").startswith("registry+")]
    queries = [
        {"package": {"name": package["name"], "ecosystem": "crates.io"}, "version": package["version"]}
        for package in packages
    ]
    results = _osv_request(OSV_QUERY_BATCH, {"queries": queries})["results"]
    findings = []
    notices = []
    for package, result in zip(packages, results):
        vulnerabilities = result.get("vulns") or []
        if not vulnerabilities:
            continue
        finding = Finding("Rust", package["name"], package["version"])
        for vulnerability in vulnerabilities:
            details = _osv_request(OSV_VULNERABILITY.format(vulnerability["id"]))
            affected = [
                entry
                for entry in details.get("affected", [])
                if entry.get("package", {}).get("name") == package["name"]
            ]
            if any((entry.get("database_specific") or {}).get("informational") == "unmaintained" for entry in affected):
                notices.append(Notice(package["name"], package["version"], details["id"], details.get("summary", "")))
                continue
            finding.identifiers.add(details["id"])
            if details.get("summary"):
                finding.summaries.add(details["summary"])
            for entry in affected:
                for version_range in entry.get("ranges", []):
                    for event in version_range.get("events", []):
                        if "fixed" in event:
                            finding.fixed_versions.add(event["fixed"])
        if finding.identifiers:
            findings.append(finding)
    return findings, notices


def _audit(root: Path) -> tuple[list[Finding], list[Notice]]:
    rust_findings, notices = _rust_findings(root)
    return [*_python_findings(root), *rust_findings], notices


def _upgrade(root: Path, findings: list[Finding]) -> None:
    python_packages = sorted({finding.package for finding in findings if finding.ecosystem == "Python"})
    rust_packages = sorted({finding.package for finding in findings if finding.ecosystem == "Rust"})
    if python_packages:
        command = ["uv", "lock"]
        for package in python_packages:
            command += ["--upgrade-package", package]
        subprocess.run(command, cwd=root, check=True)
    if rust_packages:
        command = ["cargo", "update", "--manifest-path", str(root / "Cargo.toml")]
        for package in rust_packages:
            command += ["--package", package]
        subprocess.run(command, cwd=root, check=True)


def _describe(finding: Finding) -> list[str]:
    fixed = ", ".join(sorted(finding.fixed_versions)) or "no fixed version yet"
    lines = [
        f"- {finding.ecosystem} {finding.package} {finding.version}: fixed in {fixed}",
        f"  {', '.join(sorted(finding.identifiers))}",
    ]
    lines += [f"  {summary}" for summary in sorted(finding.summaries)]
    return lines


def _report(findings: list[Finding], notices: list[Notice]) -> str:
    ordered = sorted(findings, key=lambda item: (item.ecosystem, item.package, item.version))
    unaccepted = [finding for finding in ordered if finding.acceptance() is None]
    accepted = [finding for finding in ordered if finding.acceptance() is not None]
    lines = []
    if unaccepted:
        lines += [f"{len(unaccepted)} locked dependencies have known vulnerabilities:", ""]
        for finding in unaccepted:
            lines += _describe(finding)
    else:
        lines.append("No unaccepted vulnerabilities in the locked Python and Rust dependencies.")
    if accepted:
        lines += ["", f"{len(accepted)} accepted:", ""]
        for finding in accepted:
            lines += _describe(finding)
            lines.append(f"  Accepted: {finding.acceptance().reason}")
    if notices:
        lines += ["", f"{len(notices)} unmaintained, with no fix to apply:", ""]
        for notice in sorted(notices, key=lambda item: (item.package, item.version, item.identifier)):
            lines.append(f"- Rust {notice.package} {notice.version}: {notice.identifier} {notice.summary}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check uv.lock and the workspace Cargo.lock for known vulnerabilities."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="upgrade each vulnerable package in its lockfile, then audit again",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="checkout whose lockfiles to audit",
    )
    arguments = parser.parse_args()
    root = arguments.root.resolve()

    findings, notices = _audit(root)
    unaccepted = [finding for finding in findings if finding.acceptance() is None]
    if unaccepted and arguments.fix:
        print(_report(findings, notices))
        print()
        _upgrade(root, unaccepted)
        findings, notices = _audit(root)
        unaccepted = [finding for finding in findings if finding.acceptance() is None]
    print(_report(findings, notices))
    return 1 if unaccepted else 0


if __name__ == "__main__":
    sys.exit(main())
