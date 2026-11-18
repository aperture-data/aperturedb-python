"""Use the SDK's dual-version HTTP connector and raw blob streaming.

Credentials come from the normal SDK profile/environment. A native profile
uses the usual HTTP(S) port unless --port is supplied. Run with --image to
upload a local image and stream its PNG response to --output-dir.
"""

import argparse
import json
from pathlib import Path
import tempfile
import uuid

from aperturedb.CommonLibrary import create_connector
from aperturedb.ConnectorRest import ConnectorRest


def create_http_connector(params):
    configured = create_connector()
    try:
        config = configured.config
        return ConnectorRest(
            host=config.host,
            port=params.port if params.port is not None else (
                config.port if config.use_rest else None),
            user=config.username, password=config.password, token=config.token,
            use_ssl=False if params.no_ssl else config.use_ssl,
            ca_cert=config.ca_cert,
            verify_hostname=False if params.no_verify_ssl else config.verify_hostname)
    finally:
        configured.close()


def add_image(client, image_path, identifier, class_name):
    command = {"properties": {"rest_api_example_id": identifier}}
    if client.hello_version >= 2:
        command["class"] = class_name
    with Path(image_path).open("rb") as source:
        response, _ = client.query([{"AddImage": command}], [source.read()])
    if client.check_status(response) < 0:
        raise RuntimeError(json.dumps(response))
    return response


def download_image(client, identifier, class_name, output_dir):
    command = {"blobs": True, "as_format": "png",
               "results": {"all_properties": True}}
    if client.hello_version >= 2:
        command.update(with_class=class_name,
                       constraints=["$rest_api_example_id", "==", identifier])
    else:
        command["constraints"] = {"rest_api_example_id": ["==", identifier]}
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pending = []
    try:
        with client.query_stream([{"FindImage": command}]) as response:
            if client.check_status(response.json) < 0:
                raise RuntimeError(json.dumps(response.json))
            print(json.dumps(response.json, indent=2))
            # Exhaust the iterator to validate MIME and HTTP completion before
            # publishing the downloaded files under their final names.
            for index, blob in enumerate(response.blobs):
                path = output_dir / f"image-{index}.png"
                with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".part",
                                                 delete=False) as destination:
                    temporary = Path(destination.name)
                    pending.append((temporary, path))
                    for chunk in blob.iter_chunks():
                        destination.write(chunk)
        for temporary, path in pending:
            temporary.replace(path)
        return [path for _, path in pending]
    finally:
        for temporary, _ in pending:
            temporary.unlink(missing_ok=True)


def main(params):
    client = create_http_connector(params)
    try:
        response, _ = client.query([{"GetStatus": {}}])
        print(json.dumps(response, indent=2))
        if params.image:
            identifier = uuid.uuid4().hex
            print(json.dumps(add_image(client, params.image, identifier,
                                       params.class_name), indent=2))
            for path in download_image(client, identifier, params.class_name,
                                       params.output_dir):
                print(f"Wrote {path}: {path.stat().st_size} raw bytes")
    finally:
        client.close()


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, help="HTTP(S) port override")
    parser.add_argument("--no-ssl", action="store_true", help="Use HTTP")
    parser.add_argument("--no-verify-ssl", action="store_true",
                        help="Disable TLS certificate/hostname verification for local testing")
    parser.add_argument("--image", type=Path,
                        help="Local image to upload and retrieve")
    parser.add_argument("--class-name", default="RestExampleImage")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("rest-api-output"))
    return parser.parse_args()


if __name__ == "__main__":
    main(get_args())
