from netaudio.common.config_loader import load_config_document
from netaudio.common.ddm_config_store import save_ddm_context, set_default_ddm_context
from netaudio.common.managed_api import resolve_ddm_configuration


def test_save_context_preserves_unrelated_configuration_and_uses_relative_credential_path(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("[ui]\nicons = true\n", encoding="utf-8")
    credential = tmp_path / "credentials" / "studio"

    save_ddm_context(
        path,
        server_name="studio",
        url="https://studio.example/graphql",
        credential_file=credential,
        context_name="studio-main",
        domain_id="domain-1",
        domain_name="Main Studio",
        make_default=True,
    )

    document = load_config_document(path)
    configuration = resolve_ddm_configuration(document, base_directory=tmp_path)
    assert document["ui"] == {"icons": True}
    assert configuration.default_context == "studio-main"
    assert configuration.servers["studio"].credential_file == credential
    assert configuration.contexts["studio-main"].domain_name == "Main Studio"


def test_save_context_updates_exact_tables_without_touching_another_server(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        """
[ddm]
default_context = "old"

[ddm.servers.studio]
url = "https://old.example/graphql"
credential_file = "old-key"
enabled = false

[ddm.servers.other]
url = "https://other.example/graphql"
credential_file = "other-key"
enabled = true

[ddm.contexts.old]
server = "studio"
domain_id = "old-domain"
""".lstrip(),
        encoding="utf-8",
    )

    save_ddm_context(
        path,
        server_name="studio",
        url="https://new.example/graphql",
        credential_file=tmp_path / "new-key",
        context_name="new",
        domain_id="new-domain",
        domain_name=None,
        make_default=False,
    )
    set_default_ddm_context(path, "new")

    document = load_config_document(path)
    assert document["ddm"]["servers"]["studio"] == {
        "url": "https://new.example/graphql",
        "credential_file": "new-key",
        "enabled": True,
    }
    assert document["ddm"]["servers"]["other"]["url"] == "https://other.example/graphql"
    assert document["ddm"]["default_context"] == "new"


def test_edit_profile_preserves_credentials_and_updates_context_references(tmp_path):
    from netaudio.common.ddm_config_store import edit_ddm_server, save_ddm_server

    path = tmp_path / "config.toml"
    path.write_text("[ui]\nicons = true\n")
    save_ddm_server(path, name="old", url="https://ddm.example/graphql", credential="synthetic-test-value")
    credential_file = (
        resolve_ddm_configuration(load_config_document(path), base_directory=tmp_path).servers["old"].credential_file
    )
    save_ddm_context(
        path,
        server_name="old",
        url="https://ddm.example/graphql",
        credential_file=credential_file,
        context_name="studio",
        domain_id="domain-1",
        domain_name="Studio",
        make_default=True,
    )
    edit_ddm_server(path, current_name="old", name="new", url="https://ddm.example/graphql")
    document = load_config_document(path)
    configuration = resolve_ddm_configuration(document, base_directory=tmp_path)
    assert set(configuration.servers) == {"new"}
    assert configuration.servers["new"].credential_file == credential_file
    assert configuration.contexts["studio"].server == "new"
    assert configuration.default_context == "studio"
    assert document["ui"] == {"icons": True}


def test_changing_profile_address_clears_authentication_and_domain_selections(tmp_path):
    from netaudio.common.ddm_config_store import edit_ddm_server, save_ddm_server

    path = tmp_path / "config.toml"
    save_ddm_server(path, name="studio", url="https://old.example/graphql", credential="synthetic-test-value")
    configuration = resolve_ddm_configuration(load_config_document(path), base_directory=tmp_path)
    save_ddm_context(
        path,
        server_name="studio",
        url="https://old.example/graphql",
        credential_file=configuration.servers["studio"].credential_file,
        context_name="main",
        domain_id="domain-1",
        domain_name="Main",
        make_default=True,
    )
    edit_ddm_server(path, current_name="studio", name="studio", url="https://new.example/graphql")
    configuration = resolve_ddm_configuration(load_config_document(path), base_directory=tmp_path)
    assert configuration.servers["studio"].credential_file is None
    assert not configuration.servers["studio"].enabled
    assert not configuration.contexts
    assert configuration.default_context is None


def test_remove_profile_preserves_other_servers(tmp_path):
    from netaudio.common.ddm_config_store import edit_ddm_server, save_ddm_server

    path = tmp_path / "config.toml"
    save_ddm_server(path, name="studio", url="https://studio.example/graphql", credential="synthetic-test-value")
    save_ddm_server(path, name="other", url="https://other.example/graphql", credential="synthetic-test-value")
    edit_ddm_server(path, current_name="studio", name=None, url=None)
    configuration = resolve_ddm_configuration(load_config_document(path), base_directory=tmp_path)
    assert set(configuration.servers) == {"other"}
    assert configuration.servers["other"].enabled
