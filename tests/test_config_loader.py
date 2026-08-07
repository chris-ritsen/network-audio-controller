from netaudio.common.config_loader import get_config_value, load_capture_profile, set_config_value


def test_top_level_application_values_are_loaded(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text('device_lock_key = "0123456789abcdef0123456789abcdef"\n')
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    configuration, loaded_path = load_capture_profile(None, None)

    assert loaded_path == config_path
    assert configuration["device_lock_key"] == "0123456789abcdef0123456789abcdef"
    assert get_config_value("device_lock_key") == (
        "0123456789abcdef0123456789abcdef",
        config_path,
    )


def test_set_and_get_top_level_application_value(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("NETAUDIO_CONFIG", str(config_path))

    assert set_config_value("device_lock_key", "0123456789abcdef0123456789abcdef") == config_path
    assert get_config_value("device_lock_key") == (
        "0123456789abcdef0123456789abcdef",
        config_path,
    )
