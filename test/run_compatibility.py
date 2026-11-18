"""Fail-closed CI entry point for real server-1.0 SDK compatibility tests.

The full historical integration suite runs separately against server 0.20.0.
This runner selects focused regression tests and requires every permanent
protocol/helper matrix test to execute successfully over native TLS and HTTPS.
"""
import json
import os
from pathlib import Path
import time


REQUIRED_TESTS = {
    "test_native_tls_http_and_streaming_matrix",
    "test_vector_helpers_metadata_schema_and_atomic_removal",
    "test_sparql_types_images_connections_and_knn",
    "test_image_similarity_and_typed_property_names",
    "test_langchain_store_upsert_retriever_and_metadata",
    "test_native_parallel_owner_references",
}
TEST_FILES = [
    "test_QueryTransport.py", "test_ServerCompatibility.py",
    "test_DescriptorEngines.py", "test_ServerVersionLoaders.py",
    "test_DatasetPaging.py", "test_SPARQLCompatibility.py",
    "test_CompatibilityRunner.py", "test_ExampleCompatibility.py", "test_Session.py",
    "test_ProtocolCompatibility.py", "test_HelperCompatibility.py",
    "test_CliGeneratedQueries.py", "test_LangChain.py", "test_ParallelReferences.py", "test_DescriptorDiversity.py",
]


def read_endpoints(value):
    """Require both verified transports and explicit hello-v2 expectations."""
    try:
        endpoints = json.loads(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "ADB_COMPAT_ENDPOINTS must contain a JSON endpoint array") from error
    if not isinstance(endpoints, list) or not endpoints:
        raise ValueError(
            "ADB_COMPAT_ENDPOINTS must be a nonempty endpoint array")
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            raise ValueError("Every compatibility endpoint must be an object")
        if type(endpoint.get("hello_version")) is not int or endpoint["hello_version"] != 2:
            raise ValueError(
                "CI compatibility endpoints must expect hello_version 2")
        if endpoint.get("use_ssl") is not True or endpoint.get("verify_hostname") is not True:
            raise ValueError(
                "CI compatibility endpoints must verify TLS hostnames")
        if not endpoint.get("host") or not endpoint.get("ca_cert"):
            raise ValueError(
                "CI compatibility endpoints require host and ca_cert")
        if type(endpoint.get("rest", False)) is not bool:
            raise ValueError("Endpoint rest must be a boolean")
        if type(endpoint.get("port")) is not int or not 0 < endpoint["port"] < 65536:
            raise ValueError("CI compatibility endpoints require a valid port")
    if {bool(endpoint.get("rest", False)) for endpoint in endpoints} != {False, True}:
        raise ValueError(
            "CI compatibility requires native TLS and HTTPS endpoints")
    return endpoints


def wait_for_endpoint(endpoint, attempts=30):
    """Allow startup connection failures before running any test queries."""
    from aperturedb.Connector import Connector
    from aperturedb.ConnectorRest import ConnectorRest

    connector = ConnectorRest if endpoint.get("rest") else Connector
    for attempt in range(attempts):
        client = connector(
            host=endpoint["host"], port=endpoint["port"],
            user=os.getenv("ADB_COMPAT_USERNAME", "admin"),
            password=os.getenv("ADB_COMPAT_PASSWORD", "admin"),
            use_ssl=True, ca_cert=endpoint["ca_cert"], verify_hostname=True,
            **({"retry_max_attempts": 1, "retry_interval_seconds": 0}
               if not endpoint.get("rest") else {}))
        try:
            try:
                version = client.hello_version
            except ConnectionError:
                if attempt + 1 == attempts:
                    raise
                time.sleep(1)
                continue
            if version != 2:
                raise RuntimeError(
                    f"{endpoint['host']}:{endpoint['port']} negotiated hello {version}; expected 2")
            return
        finally:
            client.close()


class RequiredCompatibility:
    """Prevent an accidentally skipped or deselected live matrix from passing CI."""

    def __init__(self):
        self.passed = set()

    def pytest_runtest_logreport(self, report):
        if report.when == "call" and report.passed:
            self.passed.add(report.nodeid.rsplit("::", 1)[-1])

    def pytest_sessionfinish(self, session, exitstatus):
        missing = REQUIRED_TESTS - self.passed
        if missing:
            reporter = session.config.pluginmanager.get_plugin(
                "terminalreporter")
            if reporter:
                reporter.write_line("Required live compatibility tests did not pass: " +
                                    ", ".join(sorted(missing)), red=True)
            session.exitstatus = 1


def main():
    import pytest

    endpoints = read_endpoints(os.environ.get("ADB_COMPAT_ENDPOINTS"))
    for endpoint in endpoints:
        wait_for_endpoint(endpoint)
    root = Path(__file__).resolve().parent
    arguments = [str(root / name) for name in TEST_FILES]
    arguments += ["-v", "--strict-markers", "-c", str(root / "pytest.ini"),
                  "--junitxml=" + os.getenv("ADB_COMPAT_JUNIT", "output/sdk-compat.xml")]
    return int(pytest.main(arguments, plugins=[RequiredCompatibility()]))


if __name__ == "__main__":
    raise SystemExit(main())
