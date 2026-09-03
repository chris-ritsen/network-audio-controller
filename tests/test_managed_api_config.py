from pathlib import Path

import pytest

from netaudio.common.config_loader import default_config_path, load_capture_profile, load_config_document
from netaudio.common.managed_api import resolve_ddm_configuration, resolve_managed_api_configuration


def _configuration_document(*, enabled=True, credential_file="secrets/managed-credential"):
    return {
        "ddm": {
            "default_context": "studio-main",
            "servers": {
                "studio": {
                    "url": "http://manager.example/graphql",
                    "credential_file": credential_file,
                    "enabled": enabled,
                }
            },
            "contexts": {
                "studio-main": {
                    "server": "studio",
                    "domain_id": "domain-1",
                    "domain_name": "Studio",
                }
            },
        }
    }


def test_ddm_configuration_loads_from_full_root_not_selected_capture_profile(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                'active_profile = "studio"',
                "",
                "[ddm]",
                'default_context = "studio-main"',
                "",
                "[ddm.servers.studio]",
                'url = "http://manager.example/graphql"',
                'credential_file = "secrets/managed-credential"',
                "enabled = true",
                "",
                "[ddm.contexts.studio-main]",
                'server = "studio"',
                'domain_id = "domain-1"',
                "",
                "[profiles.studio]",
                'interface = "eth0"',
            )
        )
    )
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    document = load_config_document()
    profile, profile_path = load_capture_profile(None, None)
    configuration = resolve_managed_api_configuration(document, base_directory=default_config_path().parent)

    assert profile == {"interface": "eth0"}
    assert profile_path == config_path
    assert configuration.name == "studio"
    assert configuration.url == "http://manager.example/graphql"
    assert configuration.credential_file == (tmp_path / "secrets" / "managed-credential").resolve()
    assert configuration.enabled is True
    assert configuration.configuration_error is None


@pytest.mark.parametrize(
    ("enabled", "expected_enabled", "expected_error"),
    ((False, False, None), (True, True, None), (None, True, None)),
)
def test_enabled_setting_controls_named_server(enabled, expected_enabled, expected_error):
    document = _configuration_document(enabled=enabled)
    if enabled is None:
        del document["ddm"]["servers"]["studio"]["enabled"]

    configuration = resolve_managed_api_configuration(document)

    assert configuration.enabled is expected_enabled
    assert configuration.configuration_error == expected_error


def test_explicit_enable_without_connection_details_is_an_error():
    document = _configuration_document(credential_file=None)
    configuration = resolve_managed_api_configuration(document)

    assert configuration.enabled is False
    assert configuration.configuration_error == "DDM server profile has a URL but no credential"


def test_ddm_configuration_must_be_a_table():
    with pytest.raises(ValueError, match="ddm must be a table"):
        resolve_ddm_configuration({"ddm": "invalid"})


@pytest.mark.parametrize("key", ["url", "credential_file", "enabled", "refresh_interval"])
def test_connection_settings_are_rejected_outside_named_server_profiles(key):
    with pytest.raises(ValueError, match="unknown ddm settings"):
        resolve_ddm_configuration({"ddm": {key: "value"}})


@pytest.mark.parametrize("key", ["credential", "api_key", "api_key_file", "refresh_interval_seconds"])
def test_obsolete_or_inline_server_settings_are_rejected(key):
    document = _configuration_document()
    document["ddm"]["servers"]["studio"][key] = "value"
    with pytest.raises(ValueError, match="unknown ddm.servers.studio settings"):
        resolve_ddm_configuration(document)


def test_named_servers_and_contexts_bind_domain_to_one_credential(tmp_path):
    configuration = resolve_ddm_configuration(
        {
            "ddm": {
                "default_context": "east-production",
                "servers": {
                    "east": {"url": "https://east.example/graphql", "credential_file": "credentials/east"},
                    "west": {"url": "https://west.example/graphql", "credential_file": "credentials/west"},
                },
                "contexts": {
                    "east-production": {
                        "server": "east",
                        "domain_id": "domain-a",
                        "domain_name": "Production",
                    },
                    "west-production": {
                        "server": "west",
                        "domain_id": "domain-b",
                        "domain_name": "Production",
                    },
                },
            }
        },
        base_directory=tmp_path,
    )

    assert set(configuration.servers) == {"east", "west"}
    assert configuration.context("east-production").domain_id == "domain-a"
    assert configuration.context("west-production").domain_id == "domain-b"
    assert configuration.selected_server().name == "east"
    assert configuration.selected_server("west-production").name == "west"
    assert configuration.servers["east"].credential_file == (tmp_path / "credentials" / "east").resolve()


def test_server_is_not_selected_without_an_explicit_or_default_context():
    document = _configuration_document()
    del document["ddm"]["default_context"]
    configuration = resolve_ddm_configuration(document)

    with pytest.raises(ValueError, match="no DDM context is selected"):
        configuration.selected_server()


@pytest.mark.parametrize(
    "document, message",
    (
        (
            {
                "ddm": {
                    "servers": {"manager": {"url": "https://manager.example/graphql", "credential_file": "key"}},
                    "contexts": {"studio": {"server": "missing", "domain_id": "domain"}},
                }
            },
            "unknown server profile",
        ),
        (
            {
                "ddm": {
                    "servers": {"manager": {"url": "https://manager.example/graphql", "credential_file": "key"}},
                    "contexts": {"studio": {"server": "manager", "domain_id": ""}},
                }
            },
            "domain_id must be a non-empty string",
        ),
    ),
)
def test_invalid_context_references_are_rejected(document, message):
    with pytest.raises(ValueError, match=message):
        resolve_ddm_configuration(document)


def test_context_names_cannot_duplicate_the_same_server_and_domain_target():
    document = {
        "ddm": {
            "servers": {"manager": {"url": "https://manager.example/graphql", "credential_file": "key"}},
            "contexts": {
                "first": {"server": "manager", "domain_id": "domain-1"},
                "second": {"server": "manager", "domain_id": "domain-1"},
            },
        }
    }

    with pytest.raises(ValueError, match="duplicates server and domain target"):
        resolve_ddm_configuration(document)


def test_no_ddm_configuration_returns_a_disabled_client_configuration():
    configuration = resolve_managed_api_configuration({}, base_directory=Path("/configuration"))
    assert configuration.enabled is False
    assert configuration.configuration_error is None
