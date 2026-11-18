# Python SDK 1.0.1 release validation

CI tests the supported ApertureDB server releases over native TCP/TLS and HTTPS.
The server 0.20.0 stack runs the integration suite, including query, CSV, and
framework tests. Pull requests use the configured slow and external test
filters. The develop, main, and nightly workflows run the full suite.

The server 1.0.0 stack runs the SDK compatibility suite with certificate and
hostname verification. The suite covers raw binary uploads, streaming,
reconnection, descriptor index creation and removal, class removal, schema
rendering, SPARQL, and image similarity. It also exercises command generation
for queries, CSVs, and model-backed loaders. Both transports must pass the
required cases. CI checks the running server's source revision and image
digest before publishing the SDK.

Run both CPU suites with a built SDK test image.

```bash
cd test
RUNNER_NAME=sdk1_validation \
ADB_COMPOSE=oci://aperturedata/aperturedb-compose-single-node:0.20.0 \
SDK_COMPAT_COMPOSE=oci://aperturedata/aperturedb-compose-single-node:1.0.0 \
SKIP_SLOW_TESTS=true ./run_test_container.sh
```

To run the server 1.0.0 suite on its own, set `RUNNER_NAME` and
`SDK_COMPAT_COMPOSE` and run `./run_compatibility_container.sh` from the
`test` directory. The JUnit report is written to
`test/output/client/sdk-compat/junit.xml`. Each runner removes its own test stack.
