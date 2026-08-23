from pathlib import Path

from netaudio.commands import capture_helpers


def test_provenance_defaults_never_write_into_the_source_tree(tmp_path, monkeypatch):
    provenance_directory = tmp_path / "application-data" / "provenance"
    monkeypatch.setattr(capture_helpers, "DEFAULT_PROVENANCE_DIRECTORY", provenance_directory)

    assert capture_helpers._default_provenance_output_dir() == provenance_directory / "fixtures"
    assert capture_helpers._default_fixture_root() == provenance_directory
    assert capture_helpers._default_label_overrides_path() == provenance_directory / "label_provenance_overrides.json"


def test_provenance_bundle_resolution_prefers_an_explicit_existing_path(tmp_path, monkeypatch):
    provenance_directory = tmp_path / "application-data" / "provenance"
    monkeypatch.setattr(capture_helpers, "DEFAULT_PROVENANCE_DIRECTORY", provenance_directory)
    explicit_bundle = tmp_path / "capture.tar.gz"
    explicit_bundle.write_bytes(b"capture")

    assert capture_helpers._resolve_provenance_bundle_path(str(explicit_bundle)) == explicit_bundle
    assert capture_helpers._resolve_provenance_bundle_path("missing.tar.gz") == (
        provenance_directory / "fixtures" / "missing.tar.gz"
    )


def test_default_fact_store_is_application_data_not_a_repository_fixture():
    from netaudio.dante.fact_store import DEFAULT_FACTS_PATH

    assert DEFAULT_FACTS_PATH == Path.home() / ".local" / "share" / "netaudio" / "provenance" / "facts.json"
