---
slug: /python_sdk/compatibility/1.0.0
sidebar_label: 1.0.0
sidebar_position: 1
---

# ApertureDB 1.0.0 compatibility

SDK 1.0.1 supports ApertureDB server 0.20.0 and 1.0.0 through native TCP/TLS
and HTTP. Upgrade the SDK while the database still runs 0.20.0, migrate custom
queries, then upgrade the server. Database upgrades must pass through 0.20.0.

| Server | Native queries | HTTP responses |
| --- | --- | --- |
| 0.20.0 | Protobuf envelope | JSON with base64 blobs |
| 1.0.0 | JSON command array and sized binary blobs | Multipart JSON and raw binary blobs |

The connector selects the transport automatically.

## Existing buffered queries

`client.query(commands, blobs)` keeps returning `(json_response, output_blobs)`.
It buffers the complete response before returning. Connection establishment can
be retried, and a complete authentication rejection can renew a session and
retry because no commands ran. A transport failure after query transmission is
never replayed automatically. Inspect committed effects before retrying a write.
Dataset wrappers follow the same rule and create independent sessions in forked
workers before sending queries.

## Stream large responses

```python
from aperturedb.CommonLibrary import create_connector

client = create_connector()
query = [{"FindImage": {"with_class": "Photos", "blobs": True}}]
with client.query_stream(query) as result:
    print(result.json)
    for index, blob in enumerate(result.blobs):
        with open(f"image-{index}.bin", "wb") as output:
            for chunk in blob.iter_chunks():
                output.write(chunk)
```

JSON metadata is eager; each output blob is read when the application advances
the iterator. `blob.read()` collects one blob, while `blob.iter_chunks()` bounds
application buffering. Finish each blob before requesting another. Exhaust the
blob iterator, including for HTTP responses with no output blobs, to validate
complete framing and the closing multipart boundary. Valid JSON alone does not
mean the operation completed successfully.

Exiting the context with unread data or receiving an incomplete blob closes the
connection. Another query on that connector is rejected while its response is
active; clone the connector for concurrent work. Late storage/encoding failures
raise `ConnectionError`. Server 0.20.0 buffers its protobuf response before
exposing the same iterator; incremental wire delivery requires server 1.0.0.
Some encoded media must materialize one object to determine its byte length.

## Classes, descriptors and helpers

Raw queries use the connected server's command API. SDK 1.0.1 does not rewrite
arbitrary raw descriptor-set commands, cached paging, or missing application
classes. Server 1.0 requires explicit classes on additions and array-expression
constraints. Result selection such as `limit` and `sort` belongs at the command
level. Owner selectors use `image_ref`/`video_ref` when finding regions or
video parts, and `bounding_box_ref`/`polygon_ref`/`frame_ref`/`clip_ref` for their
parents. Customer `connect` entries require a class; `is_connected_to` traverses
customer connections. `FindDescriptorBatch` and ingestion batch sizes remain supported.

The wrapper helpers choose equivalent forms for the connected server:

```python
from aperturedb.Utils import Utils
from aperturedb.Descriptors import Descriptors

utils = Utils(client)
assert utils.create_vector_index(
    "Embeddings", 768, metric="CS", kind="hnsw",
    metadata={"model": "example-model"})

neighbors = Descriptors(client)
neighbors.find_similar("Embeddings", vector, 10, metric="CS")
```

This creates a descriptor set on 0.20.0 and a descriptor class/native vector
index on 1.0.0. Existing `add_descriptorset`, listing, counting and removal helper
names remain available and use the appropriate server representation. In 1.0,
set properties become index `metadata`, and `FindDescriptor` uses `with_class`
in place of `set`. Searches resolve an explicit engine and metric from the
native definition; pass them to select a particular configuration. Native
index results preserve metadata, progress and endpoint selectors.

`Utils.get_schema()` returns the server's original format: 1.0 uses `nodes` and
`edges` arrays. SDK schema summaries, diagrams and SPARQL support both forms.
Type-qualified selectors such as `image:Photos` keep identically named classes
of different object types distinct. Native connection indexes require endpoint
types and classes, for example `create_connection_index("AppearsIn", "id",
src_type="entity", src_class="People", dst_type="image", dst_class="Photos")`.

## Loaders and model queries

Media CSV loaders need an application class for server 1.0:

```python
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ParallelLoader import ParallelLoader

data = ImageDataCSV("images.csv", class_name="Photos")
ParallelLoader(client).ingest(data, batchsize=32)
```

Parallel loaders select the connected server’s command format before index
setup and iteration, including sliced CSV inputs and Dask workers. Descriptor CSV `set` values name
the descriptor class on 1.0. Descriptor-set CSV rows create native vector
indexes; graph conditions and references on those index definitions cannot be
represented and are rejected. Typed connection CSV headers use selectors such
as `entity:People@id` and `image:Photos@id`.

Pass `client=client` when generating native model queries
outside wrappers. Model application classes use their Python model names, with
an optional `class_name` override. Specialized query wrappers honor
`with_class`; generic builders accept type-qualified selectors. Custom
`generate_query()` implementations still need their own explicit server-1.0
command forms.

PyTorch/TensorFlow dataset wrappers keep cached paging on 0.20.0 and use ordinary
`offset`/`limit` pages on 1.0.0. Keep their selected data fixed while iterating:
ordinary pages do not retain a snapshot across writes. Modern dataset queries
must be read-only, and their `batch_size` controls client paging without sending
the removed server `batch` parameter.
