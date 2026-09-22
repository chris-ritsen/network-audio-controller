from netaudio import core

EMPTY_BEFORE = "28090016260026000001000000000000020000000000"
AFTER_BROOKLYN_CREATE = "2809007426002600000100000000000002010020000032000000bb8000000018142600020000000300020000000000020000000000160018000f424000000000000000000000000008120000000000000001000000000000040a0101006c0000040600010002000002000010080210e1efffd392"
AFTER_BROOKLYN_DELETE = "28090016260026000001000000000000020000200000"
AFTER_CONTROLLER_CREATE = "2809007426002600000100000000000002010020120032000000bb8000000018142600020000000300020000000000020000000000160018000f424000000000000000000000000008120000000000000001000000000000040a0101006c00000406000100020000020000100802000000000000"
AFTER_CONTROLLER_DELETE = "28090016260026000001000000000000020000340037"


def test_live_aes3_2600_pages_parse_create_and_clear():
    before = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(EMPTY_BEFORE),
    )
    assert before["reported_flow_count"] == 0
    assert before["flows"] == []

    after_create = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_BROOKLYN_CREATE),
    )
    assert after_create["reported_flow_count"] == 1
    flow = after_create["flows"][0]
    assert flow["global_flow_id"] == 2
    assert flow["flow_type"] == "multicast"
    assert flow["destination_internet_protocol_version_four_address"] == "239.255.211.146"
    assert flow["destination_user_datagram_port"] == 4321

    after_delete = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_BROOKLYN_DELETE),
    )
    assert after_delete["reported_flow_count"] == 0
    assert after_delete["flows"] == []

    after_controller_create = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_CONTROLLER_CREATE),
    )
    assert after_controller_create["flows"][0]["global_flow_id"] == 2
    assert after_controller_create["flows"][0]["flow_type"] == "multicast"

    after_controller_delete = core.parse_response(
        "transmitter_flow_status_page",
        bytes.fromhex(AFTER_CONTROLLER_DELETE),
    )
    assert after_controller_delete["reported_flow_count"] == 0
