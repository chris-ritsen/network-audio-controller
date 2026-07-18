from netaudio.dante.packet_dissector import dissect


def test_sample_rate_fields_are_rendered_as_frequency():
    payload = bytearray(40)
    payload[36:40] = (44_100).to_bytes(4, "big")
    facts = [
        {
            "category": "conmon_message",
            "key": "0x0081",
            "name": "sample_rate_control",
            "fields": [
                {
                    "name": "target_sample_rate",
                    "offset": 36,
                    "length": 4,
                    "dtype": "uint32_be",
                }
            ],
        }
    ]

    result = dissect(bytes(payload), facts=facts)

    target_sample_rate = next(span for span in result.spans if span.name == "target_sample_rate")
    assert target_sample_rate.value == "44,100 (44.1 kHz)"


def test_rx_subscription_status_uses_capture_backed_labels():
    payload = bytearray(32)
    payload[0:2] = (0x27FF).to_bytes(2, "big")
    payload[2:4] = len(payload).to_bytes(2, "big")
    payload[6:8] = (0x3000).to_bytes(2, "big")
    payload[8:10] = (0x0001).to_bytes(2, "big")
    payload[10] = 1
    payload[11] = 1
    payload[12:14] = (1).to_bytes(2, "big")
    payload[26:28] = (0x0004).to_bytes(2, "big")

    result = dissect(bytes(payload), facts=[])

    subscription_status = next(span for span in result.spans if span.name == "subscription_status")
    assert subscription_status.detail == "Subscription connected (self)"
