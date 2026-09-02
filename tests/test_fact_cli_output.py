import json

from typer.testing import CliRunner

from netaudio.cli import OutputFormat, state
from netaudio.commands.fact import cli as fact_commands
from netaudio.commands.fact import lifecycle as fact_lifecycle
from netaudio.dante.fact_store import FactRecord, add_fact

runner = CliRunner()


def _facts(tmp_path, monkeypatch):
    facts_path = tmp_path / "facts.json"
    add_fact(
        facts_path,
        "arc_opcode",
        "0x1003",
        FactRecord(
            "device_info",
            confidence="observed",
            evidence=["session_one:4"],
            fields=[{"direction": "response", "dtype": "u16", "length": 2, "name": "status", "offset": 6}],
            note="Returns device information.",
        ),
    )
    add_fact(facts_path, "conmon_message", "0x0081", FactRecord("gain_control", confidence="inferred"))
    monkeypatch.setattr(fact_commands, "_resolve_facts_path", lambda: facts_path)
    monkeypatch.setattr(fact_lifecycle, "_resolve_facts_path", lambda: facts_path)
    monkeypatch.setattr(state, "output_format", OutputFormat.plain)
    monkeypatch.setattr(state, "verbose", False)
    return facts_path


def test_fact_list_plain_output_groups_by_category(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    result = runner.invoke(fact_commands.app, ["list"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "Categories: arc_opcode, conmon_message",
        "",
        "[arc_opcode]",
        "  ○ 0x1003           device_info",
        "[conmon_message]",
        "  ~ 0x0081           gain_control",
        "",
        "2 facts (all categories)",
    ]


def test_fact_list_json_output_returns_facts(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)
    result = runner.invoke(fact_commands.app, ["list", "--category", "arc_opcode"])
    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["categories"] == ["arc_opcode", "conmon_message"]
    assert [fact["key"] for fact in document["facts"]] == ["0x1003"]


def test_fact_show_plain_output_lists_fields_and_evidence(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    result = runner.invoke(fact_commands.app, ["show", "--category", "arc_opcode", "--key", "0x1003"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "Fact: arc_opcode:0x1003",
        "  Name:       device_info",
        "  Confidence: observed",
        "  Note:       Returns device information.",
        "  Fields:",
        "    [response] status               offset    6  2B  u16",
        "  Evidence:",
        "    session_one:4",
    ]


def test_fact_show_json_output_includes_proof_errors(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)
    result = runner.invoke(
        fact_commands.app,
        ["show", "--category", "arc_opcode", "--key", "0x1003", "--prove", "--provenance-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["fact"]["key"] == "0x1003"
    assert document["proof"][0]["error"] == "bundle not found: session_one"


def test_fact_check_json_output_summarises_results(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)
    result = runner.invoke(fact_commands.app, ["check", "--provenance-dir", str(tmp_path)])
    document = json.loads(result.output)
    assert document["summary"]["total"] == 2
    assert {entry["status_label"] for entry in document["results"]} <= {"FAIL", "WARN", "PASS"}
    assert result.exit_code == (1 if document["summary"]["failed"] else 0)


def test_fact_disprove_json_output_returns_fact(tmp_path, monkeypatch):
    _facts(tmp_path, monkeypatch)
    monkeypatch.setattr(state, "output_format", OutputFormat.json)
    result = runner.invoke(
        fact_commands.app,
        ["disprove", "--category", "conmon_message", "--key", "0x0081", "--reason", "wrong opcode"],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["fact"]["key"] == "0x0081"
