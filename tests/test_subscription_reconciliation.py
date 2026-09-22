import pytest

from netaudio import core


def request(protocol=0x27FF, managed=False):
    return {
        "protocol_id": protocol,
        "managed": managed,
        "channels": [{"number": number, "media_type_code": 3} for number in range(1, 35)],
        "subscriptions": [{"number": 1, "tx_channel": "Mic", "tx_device": "Source"}],
        "expected": [{"number": number, "source": ["Mic", "Source"]} for number in range(1, 35)],
    }


@pytest.mark.parametrize(
    "protocol,managed,sizes", [(0x27FF, False, [16, 16, 1]), (0x280F, False, [32, 1]), (0x27FF, True, [32, 1])]
)
def test_reconciliation_skips_satisfied_routes_and_bounds_pending_work(protocol, managed, sizes):
    plan = core.plan_subscription_reconciliation(request(protocol, managed))

    assert plan["unchanged"] == [{"number": 1, "source": ["Mic", "Source"]}]
    assert [len(batch["expected"]) for batch in plan["batches"]] == sizes
    assert [entry["number"] for batch in plan["batches"] for entry in batch["expected"]] == list(range(2, 35))
    assert all(batch["action"] == "set" for batch in plan["batches"])


def test_reconciliation_clears_before_setting_and_normalizes_empty_sources():
    spec = request()
    spec["subscriptions"].append({"number": 34, "tx_channel": "Old", "tx_device": "Source"})
    spec["expected"][-1]["source"] = ["", ""]
    plan = core.plan_subscription_reconciliation(spec)

    assert plan["batches"][0] == {"action": "clear", "expected": [{"number": 34, "source": None}]}
    assert all(batch["action"] == "set" for batch in plan["batches"][1:])


@pytest.mark.parametrize(
    "invalid", ["revision", "duplicate", "partial_source", "missing_channel", "last_batch", "missing_media"]
)
def test_reconciliation_rejects_entire_intent_before_any_work_escapes(invalid):
    spec = request(0x280F)

    if invalid == "revision":
        spec["protocol_id"] = 0x2810
    elif invalid == "duplicate":
        spec["expected"].append(spec["expected"][0])
    elif invalid == "partial_source":
        spec["expected"][-1]["source"] = ["", "Source"]
    elif invalid == "missing_channel":
        spec["channels"].pop()
    elif invalid == "last_batch":
        spec["expected"][-1]["source"] = ["Mic", "x" * 100]
    else:
        spec["channels"][-1]["media_type_code"] = None

    with pytest.raises(core.NetaudioCoreError):
        core.plan_subscription_reconciliation(spec)


def test_reconciliation_has_no_work_for_already_satisfied_or_empty_intent():
    spec = request()
    spec["expected"] = spec["expected"][:1]
    assert core.plan_subscription_reconciliation(spec)["batches"] == []

    spec["expected"] = []
    assert core.plan_subscription_reconciliation(spec) == {"unchanged": [], "batches": []}
