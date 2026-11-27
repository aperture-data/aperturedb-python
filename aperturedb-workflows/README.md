# ApertureDB workflows runtime

`aperturedb-workflows` is the runtime shared by ApertureDB workflow images.
It provides the status server, the database setup that runs before an
application, argument validation and the shared Python helpers.

It is released with the SDK under the same version. The wheel is published
as the OCI artifact `aperturedata/aperturedb-workflows:<version>`, next to
`aperturedata/aperturedb-python:<version>`. Infra retags both as `dev` and
`latest`.

## Install

Pull both wheels with ORAS 1.3 and install them together:

```bash
oras pull --output wheels registry-1.docker.io/aperturedata/aperturedb-python:latest
oras pull --output wheels registry-1.docker.io/aperturedata/aperturedb-workflows:latest
pip install wheels/aperturedb-*.whl wheels/aperturedb_workflows-*.whl
```

The `embeddings` and `text` extras add the model and document
dependencies. Install the CPU build of torch first for images without a GPU:

```bash
pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision
pip install "aperturedb-workflows[embeddings,text]"
```

With the wheel already installed, pip only adds the extra dependencies.

## Image layout

An application image copies its code into `/app`, including `app.sh`, and
runs `wf-start`. Set `WORKFLOW_VERSION` to the image version.

`wf-start` starts the status server and configures the database connection. It
then runs `/app/app.sh` with `/app` as the working directory and on
`PYTHONPATH`. Uncaught Python exceptions in that run are reported to the status
server as workflow errors. An application can replace the default
`/reconfigure` handler with a `reconfigure.py` in `/app`.

| Command | Purpose |
|---|---|
| `wf-start` | Image entry point |
| `wf-argparse` | Validate and print a value or environment variable |
| `wf-status` | Report progress, phases and errors to the status server |
| `wf-log-processor` | Run a command and report the `Progress:` lines it writes to stderr |
| `wf-userlog` | Write a user log message to the database |
| `wf-slack-alert` | Post a Slack message |
| `wf-status-server` | Status server started by `wf-start` |

Python code imports the helpers from the package, for example
`from aperturedb_workflows.connection_pool import ConnectionPool`.

## Test

`bash aperturedb-workflows/test.sh` builds both wheels from this checkout and
tests them together in a Python 3.10 container.
