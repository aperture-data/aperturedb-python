#!/bin/bash
# Start or stop an ApertureDB test stack from a published Compose artifact.
#
#   stack.sh up <project>     start the stack
#   stack.sh down <project>   stop it and delete its files
#
# ADB_COMPOSE selects the artifact; the default is the latest release. Each
# project keeps its files under aperturedb/<project>/:
#   ca/ca.crt    the stack's generated certificate, valid for lenz,
#                http-proxy and localhost; clients use it as their CA
#   data/        database and server logs (data/aperturedb_logs)
# Clients on the <project>_default network connect to lenz:55551 (native
# protocol) and https://http-proxy:8443/api/ (REST). Unless PUBLISH_PORTS is
# false, the stack also publishes ADB_TCP_PORT (55555), HTTP_PORT (80) and
# HTTPS_PORT (443) on GATEWAY (127.0.0.1).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
ADB_COMPOSE=${ADB_COMPOSE:-oci://aperturedata/aperturedb-compose-single-node:latest}
CLEANUP_IMAGE=${CLEANUP_IMAGE:-alpine:3}

usage="usage: stack.sh up|down <project>"
action=${1:?$usage}
project=${2:?$usage}
# Compose project names; this also keeps the project inside aperturedb/.
case "${project}" in
    [a-z0-9]*) ;;
    *) echo "Project names start with a lowercase letter or digit." >&2; exit 2 ;;
esac
case "${project}" in
    *[!a-z0-9_-]*) echo "Project names use lowercase letters, digits, - and _." >&2; exit 2 ;;
esac
root="${SCRIPT_DIR}/aperturedb/${project}"

export DATA_DIR="${root}/data"
export GATEWAY=${GATEWAY:-127.0.0.1}

files=(-f "${ADB_COMPOSE}")
if [ "${PUBLISH_PORTS:-true}" == "false" ]; then
    files+=(-f "${SCRIPT_DIR}/no-ports.yml")
fi

compose() {
    docker compose -p "${project}" "${files[@]}" "$@"
}

# The server writes its files as root; delete them from a container.
remove_files() {
    if [ -e "${root}" ]; then
        docker run --rm -v "${SCRIPT_DIR}/aperturedb:/stacks" --entrypoint rm \
            "${CLEANUP_IMAGE}" -rf "/stacks/${project}"
    fi
}

case "${action}" in
    up)
        compose down --volumes --remove-orphans > /dev/null 2>&1 || true
        remove_files
        mkdir -p "${root}/ca" "${DATA_DIR}/aperturedb_db" "${DATA_DIR}/aperturedb_logs"
        compose pull --quiet
        # A remote artifact asks to confirm its variables; CI has no terminal.
        compose up --detach --yes --quiet-pull
        compose cp lenz:/etc/lenz/certificate/tls.crt "${root}/ca/ca.crt" > /dev/null
        chmod 644 "${root}/ca/ca.crt"
        ;;
    down)
        compose down --volumes --remove-orphans
        remove_files
        ;;
    *)
        echo "${usage}" >&2
        exit 2
        ;;
esac
