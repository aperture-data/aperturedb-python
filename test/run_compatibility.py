"""Run the required SDK integration suite over native TLS and HTTPS."""
from contextlib import closing
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
    "test_pool_borrow_return",
    "test_live_csv_indexing_and_ingestion",
}
TEST_FILES = [
    "test_Configuration.py", "test_Connector.py", "test_SessionRecovery.py",
    "test_DaskSessions.py", "test_SessionSecurity.py", "test_UserConvenience.py",
    "test_Constraints.py", "test_Operations.py", "test_Sort.py",
    "test_ConnectionPool.py", "test_QueryTransport.py", "test_ServerCompatibility.py",
    "test_DescriptorEngines.py", "test_ServerVersionLoaders.py", "test_EntityDataCSV.py",
    "test_DatasetPaging.py", "test_SPARQLCompatibility.py",
    "test_CompatibilityRunner.py", "test_ExampleCompatibility.py", "test_Session.py",
    "test_ProtocolCompatibility.py", "test_HelperCompatibility.py",
    "test_CliGeneratedQueries.py", "test_LangChain.py", "test_ParallelReferences.py",
    "test_DescriptorDiversity.py", "test_Parallel.py", "test_ResponseHandler.py",
    "test_Datawizard.py::TestDataWizardEntities", "test_Datawizard.py::TestQueryBuilder",
    "test_Server.py::TestBadResponses::test_AuthFailure", "test_Images.py",
    "test_Key.py", "test_Success.py", "test_Stats.py", "test_Utils.py",
    "test_CLI.py", "test_Sources.py", "test_tf_connector.py", "test_torch_connector.py",
    "test_Data.py::TestEntityLoader::test_BlobLoader",
    "test_Data.py::TestEntityLoader::test_updateif_fails",
    "test_Data.py::TestEntityLoader::test_updateif_passes",
    "test_Data.py::TestEntityLoader::test_updateif_partial_age",
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
    """Wait for an authenticated query to reach the database through the gateway."""
    from aperturedb.Connector import Connector
    from aperturedb.ConnectorRest import ConnectorRest

    connector = ConnectorRest if endpoint.get("rest") else Connector
    for attempt in range(attempts):
        try:
            with closing(connector(
                host=endpoint["host"], port=endpoint["port"],
                user=os.getenv("ADB_COMPAT_USERNAME", "admin"),
                password=os.getenv("ADB_COMPAT_PASSWORD", "admin"),
                use_ssl=True, ca_cert=endpoint["ca_cert"], verify_hostname=True,
                **({"retry_max_attempts": 1, "retry_interval_seconds": 0}
                   if not endpoint.get("rest") else {})
            )) as client:
                version = client.hello_version
                if version != 2:
                    raise RuntimeError(
                        f"{endpoint['host']}:{endpoint['port']} negotiated hello {version}; expected 2")
                response, _ = client.query([{"GetStatus": {}}])
                if (isinstance(response, list) and len(response) == 1
                        and isinstance(response[0], dict)
                        and isinstance(response[0].get("GetStatus"), dict)
                        and response[0]["GetStatus"].get("status") == 0):
                    return
                raise ConnectionError(
                    f"GetStatus is not ready on {endpoint['host']}")
        except ConnectionError:
            if attempt + 1 == attempts:
                raise
            time.sleep(1)


class RequiredCompatibility:
    """Prevent an accidentally skipped or deselected live matrix from passing CI."""

    def __init__(self, endpoints=()):
        self.endpoints = endpoints
        self.passed = set()

    def pytest_sessionstart(self, session):
        # pytest-cov is active before the readiness probe imports the SDK.
        for endpoint in self.endpoints:
            wait_for_endpoint(endpoint)

        from generateInput import generate_partial_load, generate_person_csv, generate_update_person
        generate_person_csv(2)
        generate_partial_load()
        generate_update_person()

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
    root = Path(__file__).resolve().parent
    arguments = [str(root / name) for name in TEST_FILES]
    arguments += [
        "-v", "--strict-markers", "-c", str(root / "pytest.ini"),
        "--cov=" + str(root.parent / "aperturedb"), "--cov-report=html:output",
        "--junitxml=" + os.getenv("ADB_COMPAT_JUNIT", "output/sdk-compat.xml"),
    ]
    result = int(pytest.main(
        arguments, plugins=[RequiredCompatibility(endpoints)]))
    if result == 0:
        from adb_timing_tests import check_cli_timing
        check_cli_timing()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
