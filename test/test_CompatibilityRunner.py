"""Regression tests for the mandatory CI compatibility gate."""
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from run_compatibility import REQUIRED_TESTS, RequiredCompatibility, read_endpoints, wait_for_endpoint


def endpoints():
    common = dict(hello_version=2, use_ssl=True,
                  verify_hostname=True, ca_cert="/ca/ca.crt")
    return [dict(common, host="lenz", port=55551),
            dict(common, host="http-proxy", port=8443, rest=True)]


@pytest.mark.parametrize("value", [None, "", "[]", "{}", "null"])
def test_missing_or_empty_endpoint_configuration_cannot_pass_ci(value):
    with pytest.raises(ValueError):
        read_endpoints(value)


@pytest.mark.parametrize("key,value", [
    ("hello_version", 1), ("hello_version", True),
    ("use_ssl", False), ("verify_hostname", False),
    ("ca_cert", ""), ("port", 0), ("rest", "false"),
])
def test_ci_rejects_legacy_plaintext_or_invalid_endpoints(key, value):
    config = endpoints()
    config[0][key] = value
    with pytest.raises(ValueError):
        read_endpoints(json.dumps(config))


def test_both_verified_transports_are_required():
    config = endpoints()
    with pytest.raises(ValueError, match="native TLS and HTTPS"):
        read_endpoints(json.dumps(config[:1]))
    assert read_endpoints(json.dumps(config)) == config


@pytest.mark.parametrize("missing", [None, *sorted(REQUIRED_TESTS)])
def test_skipped_or_deselected_live_matrix_fails_the_gate(missing):
    gate = RequiredCompatibility()
    for name in REQUIRED_TESTS - {missing}:
        gate.pytest_runtest_logreport(SimpleNamespace(
            nodeid="matrix.py::LiveCompatibility::" + name,
            when="call", passed=True))
    if missing:
        gate.pytest_runtest_logreport(SimpleNamespace(
            nodeid="matrix.py::LiveCompatibility::" + missing,
            when="call", passed=False))
    session = SimpleNamespace(exitstatus=0, config=SimpleNamespace(
        pluginmanager=SimpleNamespace(get_plugin=lambda name: None)))
    gate.pytest_sessionfinish(session, 0)
    assert session.exitstatus == (1 if missing else 0)


@pytest.mark.parametrize("first", [
    ConnectionError("warming up"),
    {"status": -1},
    [{"GetStatus": {"status": -1}}],
])
def test_readiness_retries_transient_failures_and_closes_clients(first):
    ready = [{"GetStatus": {"status": 0}}]
    clients = [Mock(hello_version=2), Mock(hello_version=2)]
    if isinstance(first, Exception):
        clients[0].query.side_effect = first
    else:
        clients[0].query.return_value = (first, [])
    clients[1].query.return_value = (ready, [])
    with patch("aperturedb.Connector.Connector", side_effect=clients), patch("run_compatibility.time.sleep"):
        wait_for_endpoint(endpoints()[0], attempts=2)
    for client in clients:
        client.close.assert_called_once()


def test_readiness_fails_on_rejected_credentials_or_wrong_protocol():
    for version, failure in [(1, "expected 2"), (2, "credential rejected")]:
        client = Mock(hello_version=version)
        client.query.side_effect = RuntimeError("credential rejected")
        with patch("aperturedb.Connector.Connector", return_value=client) as factory:
            with pytest.raises(RuntimeError, match=failure):
                wait_for_endpoint(endpoints()[0], attempts=3)
        factory.assert_called_once()
        client.close.assert_called_once()
