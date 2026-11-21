"""Regression tests for the mandatory CI compatibility gate."""
import json
from types import SimpleNamespace

import pytest

from run_compatibility import REQUIRED_TESTS, RequiredCompatibility, read_endpoints


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
