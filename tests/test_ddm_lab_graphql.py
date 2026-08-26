from __future__ import annotations

import json
from collections import deque

import pytest

from tools.ddm_lab.graphql import (
    DDMGraphQLClient,
    DDMGraphQLError,
    GraphQLResponseError,
    HTTPResponse,
    HTTPStatusError,
    ReadOnlyOperation,
)


KEY = "lab-api-key"


class FakeTransport:
    def __init__(self, *responses: HTTPResponse):
        self.responses = deque(responses)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        return self.responses.popleft()


def _response(payload, status=200):
    return HTTPResponse(status=status, body=json.dumps(payload).encode("utf-8"))


def _client(tmp_path, *responses):
    key = tmp_path / "key"
    key.write_text(KEY + "\n", encoding="ascii")
    transport = FakeTransport(*responses)
    return DDMGraphQLClient("http://ddm.local/graphql", key, transport=transport), transport


@pytest.mark.parametrize(
    ("method", "operation_name"),
    [
        ("health", "NetAudioDDMHealth"),
        ("schema", "NetAudioDDMSchema"),
        ("inventory", "NetAudioDDMInventory"),
    ],
)
def test_fixed_queries_use_raw_authorization_and_never_embed_the_key(tmp_path, method, operation_name):
    client, transport = _client(tmp_path, _response({"data": {"ok": True}}))

    result = getattr(client, method)()

    request = transport.requests[0]
    payload = json.loads(request.body)
    assert request.headers["Authorization"] == KEY
    assert request.headers["Authorization"] != f"Bearer {KEY}"
    assert KEY.encode() not in request.body
    assert payload["operationName"] == operation_name
    assert payload["query"].lstrip().startswith("query ")
    assert result.data == {"ok": True}


def test_inventory_contains_only_the_proven_read_roots(tmp_path):
    client, transport = _client(tmp_path, _response({"data": {"domains": [], "unenrolledDevices": []}}))

    client.inventory()

    query = json.loads(transport.requests[0].body)["query"]
    assert "domains" in query
    assert "unenrolledDevices" in query
    assert "macAddress" in query
    assert "mutation " not in query


def test_graphql_partial_data_is_preserved(tmp_path):
    client, _ = _client(
        tmp_path,
        _response({"data": {"domains": []}, "errors": [{"message": "one field failed"}]}),
    )

    result = client.inventory()

    assert result.partial is True
    assert result.data == {"domains": []}
    assert result.errors == ({"message": "one field failed"},)


def test_graphql_errors_without_data_fail(tmp_path):
    client, _ = _client(tmp_path, _response({"errors": [{"message": "failed"}]}))

    with pytest.raises(GraphQLResponseError, match="without usable data"):
        client.health()


def test_http_and_json_failures_are_distinct(tmp_path):
    http_client, _ = _client(tmp_path, _response({"error": "no"}, status=403))
    json_client, _ = _client(tmp_path, HTTPResponse(status=200, body=b"not json"))

    with pytest.raises(HTTPStatusError) as status:
        http_client.health()
    assert status.value.status == 403
    with pytest.raises(DDMGraphQLError, match="invalid JSON"):
        json_client.health()


def test_response_size_is_bounded_even_for_fake_transports(tmp_path):
    client, _ = _client(tmp_path, HTTPResponse(status=200, body=b"x" * 33))
    client.max_response_bytes = 32

    with pytest.raises(DDMGraphQLError, match="configured limit"):
        client.execute(ReadOnlyOperation.HEALTH)


def test_malformed_key_is_rejected_without_echoing_it(tmp_path):
    secret = "lab-api-key\r\ninjected"
    key = tmp_path / "key"
    key.write_text(secret, encoding="ascii")
    client = DDMGraphQLClient(
        "http://ddm.local/graphql",
        key,
        transport=FakeTransport(_response({"data": {"ok": True}})),
    )

    with pytest.raises(DDMGraphQLError) as failure:
        client.health()

    assert secret not in str(failure.value)


@pytest.mark.parametrize(
    "url",
    ["ftp://ddm.local/graphql", "http://ddm.local/", "http://user:pass@ddm.local/graphql"],
)
def test_endpoint_must_be_the_public_graphql_path(tmp_path, url):
    key = tmp_path / "key"
    key.write_text(KEY)
    with pytest.raises(ValueError):
        DDMGraphQLClient(url, key)
