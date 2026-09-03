from netaudio.common.config_loader import load_config_document
from netaudio.common.ddm_config_store import save_ddm_context, set_default_ddm_context
from netaudio.common.managed_api import resolve_ddm_configuration


def test_save_context_preserves_unrelated_configuration_and_uses_relative_credential_path(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text('[ui]\nicons = true\n', encoding="utf-8")
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
    configuration = resolve_ddm_configuration(document, environ={}, base_directory=tmp_path)
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
api_key = "remove-me"

[ddm.servers.other]
url = "https://other.example/graphql"
api_key_file = "other-key"

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
