import netaudio.dante.device


def func(x):
    return x + 1


def test_answer():
    assert func(4) == 5


def test_command_aes67():
    some_dev = netaudio.dante.device.DanteDevice()
    got = some_dev.command_enable_aes67(is_enabled=True)
    want = (
        "ffff002400ff22dc525400385eba0000417564696e617465073410060000006400010001",
        None,
        8700,
    )
    assert got == want

    got = some_dev.command_enable_aes67(is_enabled=False)
    want = (
        "ffff002400ff22dc525400385eba0000417564696e617465073410060000006400010000",
        None,
        8700,
    )
    assert got == want
