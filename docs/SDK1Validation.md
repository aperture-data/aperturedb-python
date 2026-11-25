# Python SDK release validation

Pull requests, develop and main integration tests all use the
infra-promoted `aperturedata/aperturedb-compose-single-node:latest` artifact.
The runner resolves the database image digest before startup, verifies the
running image against that digest and records the server's source revision.
Before publishing, SDK main also checks that the server was built from main.

A single stack serves native TLS and HTTPS with certificate and hostname
verification. Both transports exercise authentication, binary uploads,
streaming, reconnection, descriptors, graph queries and image similarity.
The same run also checks native connection pooling, SDK session handling,
CSV ingestion and indexing, process-worker reauthentication, model-backed
helpers and CLI timing for both connection profiles.

Run the integration suite from the repository root with a test image built from
the current checkout.

```bash
RUNNER_NAME=sdk_validation ./test/run_compatibility_container.sh
```

Local runs default to `oci://aperturedata/aperturedb-compose-single-node:latest`.
Set `ADB_COMPOSE` to select another published artifact. The runner writes the
image identity to `qualification/sdk-modern-stack.json`, the JUnit report to
`qualification/sdk-compat/junit.xml` and HTML coverage to `test/output/index.html`.
