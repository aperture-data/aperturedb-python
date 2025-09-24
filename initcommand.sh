#!/bin/bash

docker compose -f .devcontainer/docker-compose.yml down --remove-orphans
docker build -t aperturedata/aperturedb-notebook:dependencies docker/dependencies
echo RUNNER_NAME=devcontainer > ./.devcontainer/.env
echo ADB_REPO=aperturedata/aperturedb-community >> ./.devcontainer/.env
echo ADB_TAG=latest >> ./.devcontainer/.env
echo LENZ_REPO=aperturedata/lenz >> ./.devcontainer/.env
echo LENZ_TAG=latest >> ./.devcontainer/.env
echo GATEWAY=0.0.0.0 >> ./.devcontainer/.env
echo DB_TCP_CN=lenz >> ./.devcontainer/.env
echo DB_HTTP_CN=nginx >> ./.devcontainer/.env