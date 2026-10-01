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
CORE_MANIFEST = ROOT / "packages" / "netaudio-core" / "Cargo.toml"
CORE_LOCK = ROOT / "packages" / "netaudio-core" / "Cargo.lock"
OSV_QUERY_BATCH = "https://api.osv.dev/v1/querybatch"
OSV_VULNERABILITY = "https://api.osv.dev/v1/vulns/{}"


@dataclass
class Finding:
    ecosystem: str
    package: str
    version: str
    identifiers: set[str] = field(default_factory=set)
    fixed_versions: set[str] = field(default_factory=set)
    summaries: set[str] = field(default_factory=set)


def _python_findings() -> list[Finding]:
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
        cwd=ROOT,
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


def _rust_findings() -> list[Finding]:
    lock = tomllib.loads(CORE_LOCK.read_text())
    packages = [package for package in lock["package"] if package.get("source", "").startswith("registry+")]
    queries = [
        {"package": {"name": package["name"], "ecosystem": "crates.io"}, "version": package["version"]}
        for package in packages
    ]
    results = _osv_request(OSV_QUERY_BATCH, {"queries": queries})["results"]
    findings = []
    for package, result in zip(packages, results):
        vulnerabilities = result.get("vulns") or []
        if not vulnerabilities:
            continue
        finding = Finding("Rust", package["name"], package["version"])
        for vulnerability in vulnerabilities:
            details = _osv_request(OSV_VULNERABILITY.format(vulnerability["id"]))
            finding.identifiers.add(details["id"])
            if details.get("summary"):
                finding.summaries.add(details["summary"])
            for affected in details.get("affected", []):
                if affected.get("package", {}).get("name") != package["name"]:
                    continue
                for version_range in affected.get("ranges", []):
                    for event in version_range.get("events", []):
                        if "fixed" in event:
                            finding.fixed_versions.add(event["fixed"])
        findings.append(finding)
    return findings


def _audit() -> list[Finding]:
    return [*_python_findings(), *_rust_findings()]


def _upgrade(findings: list[Finding]) -> None:
    python_packages = sorted({finding.package for finding in findings if finding.ecosystem == "Python"})
    rust_packages = sorted({finding.package for finding in findings if finding.ecosystem == "Rust"})
    if python_packages:
        command = ["uv", "lock"]
        for package in python_packages:
            command += ["--upgrade-package", package]
        subprocess.run(command, cwd=ROOT, check=True)
    if rust_packages:
        command = ["cargo", "update", "--manifest-path", str(CORE_MANIFEST)]
        for package in rust_packages:
            command += ["--package", package]
        subprocess.run(command, cwd=ROOT, check=True)


def _report(findings: list[Finding]) -> str:
    if not findings:
        return "No known vulnerabilities in the locked Python and Rust dependencies."
    lines = [f"{len(findings)} locked dependencies have known vulnerabilities:", ""]
    for finding in sorted(findings, key=lambda item: (item.ecosystem, item.package)):
        fixed = ", ".join(sorted(finding.fixed_versions)) or "no fixed version yet"
        lines.append(f"- {finding.ecosystem} {finding.package} {finding.version}: fixed in {fixed}")
        lines.append(f"  {', '.join(sorted(finding.identifiers))}")
        for summary in sorted(finding.summaries):
            lines.append(f"  {summary}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check uv.lock and the Rust core's Cargo.lock for known vulnerabilities."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="upgrade each vulnerable package in its lockfile, then audit again",
    )
    arguments = parser.parse_args()

    findings = _audit()
    if findings and arguments.fix:
        print(_report(findings))
        print()
        _upgrade(findings)
        findings = _audit()
    print(_report(findings))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
