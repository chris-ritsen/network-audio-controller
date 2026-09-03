from pathlib import Path

import pytest

from netaudio.common.config_loader import default_config_path, load_capture_profile, load_config_document
from netaudio.common.managed_api import resolve_ddm_configuration, resolve_managed_api_configuration


def test_ddm_configuration_loads_from_full_root_not_selected_capture_profile(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            (
                'active_profile = "studio"',
                "",
                "[ddm]",
                'url = "http://manager.example/graphql"',
                'api_key_file = "secrets/managed-api-key"',
                "enabled = true",
                "",
                "[profiles.studio]",
                'interface = "eth0"',
            )
        )
    )
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    document = load_config_document()
    profile, profile_path = load_capture_profile(None, None)
    configuration = resolve_managed_api_configuration(document, environ={}, base_directory=default_config_path().parent)

    assert profile == {"interface": "eth0"}
    assert profile_path == config_path
    assert configuration.url == "http://manager.example/graphql"
    assert configuration.credential_file == (tmp_path / "secrets" / "managed-api-key").resolve()
    assert configuration.enabled is True
    assert configuration.configuration_error is None


@pytest.mark.parametrize(
    ("enabled", "expected_enabled", "expected_error"),
    (
        (False, False, None),
        (True, True, None),
        (None, True, None),
    ),
)
def test_enabled_setting_controls_complete_configuration(enabled, expected_enabled, expected_error):
    ddm = {
        "url": "http://manager.example/graphql",
        "api_key": "profile-key",
    }
    if enabled is not None:
        ddm["enabled"] = enabled

    configuration = resolve_managed_api_configuration({"ddm": ddm}, environ={})

    assert configuration.enabled is expected_enabled
    assert configuration.configuration_error == expected_error


def test_explicit_enable_without_connection_details_is_an_error():
    configuration = resolve_managed_api_configuration({"ddm": {"enabled": True}}, environ={})

    assert configuration.enabled is False
    assert configuration.configuration_error == "DDM is enabled but no URL or credential is configured"


def test_environment_overrides_endpoint_key_path_interval_and_enabled(tmp_path):
    profile = {
        "ddm": {
            "url": "http://profile.example/graphql",
            "api_key_file": "profile-key",
            "refresh_interval": 30,
            "enabled": False,
        }
    }
    environment = {
        "NETAUDIO_DDM_URL": "http://environment.example/graphql",
        "NETAUDIO_DDM_API_KEY_FILE": "secrets/environment-key",
        "NETAUDIO_DDM_REFRESH_INTERVAL": "5",
        "NETAUDIO_DDM_ENABLED": "true",
    }

    configuration = resolve_managed_api_configuration(
        profile,
        environ=environment,
        base_directory=tmp_path,
    )

    assert configuration.url == "http://environment.example/graphql"
    assert configuration.credential is None
    assert configuration.credential_file == (tmp_path / "secrets" / "environment-key").resolve()
    assert configuration.refresh_interval == 5.0
    assert configuration.enabled is True
    assert configuration.configuration_error is None


def test_inline_environment_key_supersedes_profile_key_file():
    configuration = resolve_managed_api_configuration(
        {
            "ddm": {
                "url": "http://manager.example/graphql",
                "api_key_file": "profile-key",
            }
        },
        environ={"NETAUDIO_DDM_API_KEY": "environment-key"},
        base_directory=Path("/configuration"),
    )

    assert configuration.credential == "environment-key"
    assert configuration.credential_file is None
    assert configuration.enabled is True
    assert configuration.configuration_error is None


def test_named_servers_and_contexts_bind_domain_to_one_credential(tmp_path):
    configuration = resolve_ddm_configuration(
        {
            "ddm": {
                "default_context": "east-production",
                "servers": {
                    "east": {
                        "url": "https://east.example/graphql",
                        "api_key_file": "credentials/east",
                    },
                    "west": {
                        "url": "https://west.example/graphql",
                        "api_key_file": "credentials/west",
                    },
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
        environ={},
        base_directory=tmp_path,
    )

    assert set(configuration.servers) == {"east", "west"}
    assert configuration.context("east-production").domain_id == "domain-a"
    assert configuration.context("west-production").domain_id == "domain-b"
    assert configuration.selected_server().name == "east"
    assert configuration.selected_server("west-production").name == "west"
    assert configuration.servers["east"].credential_file == (tmp_path / "credentials" / "east").resolve()


def test_multiple_servers_without_a_context_are_ambiguous():
    configuration = resolve_ddm_configuration(
        {
            "ddm": {
                "servers": {
                    "one": {"url": "https://one.example/graphql", "api_key": "one"},
                    "two": {"url": "https://two.example/graphql", "api_key": "two"},
                }
            }
        },
        environ={},
    )

    with pytest.raises(ValueError, match="select one with --context"):
        configuration.selected_server()


@pytest.mark.parametrize(
    "document, message",
    (
        (
            {
                "ddm": {
                    "servers": {"manager": {"url": "https://manager.example/graphql", "api_key": "key"}},
                    "contexts": {"studio": {"server": "missing", "domain_id": "domain"}},
                }
            },
            "unknown server profile",
        ),
        (
            {
                "ddm": {
                    "servers": {"manager": {"url": "https://manager.example/graphql", "api_key": "key"}},
                    "contexts": {"studio": {"server": "manager", "domain_id": ""}},
                }
            },
            "domain_id must be a non-empty string",
        ),
    ),
)
def test_invalid_context_references_are_rejected(document, message):
    with pytest.raises(ValueError, match=message):
        resolve_ddm_configuration(document, environ={})


def test_context_aliases_cannot_duplicate_the_same_server_and_domain_target():
    document = {
        "ddm": {
            "servers": {"manager": {"url": "https://manager.example/graphql", "api_key": "key"}},
            "contexts": {
                "first": {"server": "manager", "domain_id": "domain-1"},
                "second": {"server": "manager", "domain_id": "domain-1"},
            },
        }
    }

    with pytest.raises(ValueError, match="duplicates server and domain target"):
        resolve_ddm_configuration(document, environ={})
