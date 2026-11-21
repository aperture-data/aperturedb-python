---
slug: /python_sdk/compatibility/1.0.0
sidebar_label: Server 1.0.0
sidebar_position: 1
---

# ApertureDB 1.0.0 compatibility

SDK 1.0.2 supports ApertureDB server 0.20.0 and 1.0.0 through native TCP/TLS
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

Raw queries use the connected server's command API. SDK 1.0.2 does not rewrite
arbitrary raw descriptor-set commands, cached paging, or missing application
classes. Server 1.0 requires explicit classes on additions and array-expression
constraints. Result selection such as `limit` and `sort` belongs at the command
level. Owner selectors use `image_ref`/`video_ref` when finding regions or
video parts, and `bounding_box_ref`/`polygon_ref`/`frame_ref`/`clip_ref` for their
parents. To create a customer connection, supply its name as a string, for
example `{"AddConnection":{"class":"HasLine","src":1,"dst":2}}`. The referenced
objects determine the endpoint classes and types. Inline `connect` also takes a
string connection name and a reference, for example `{"ref":2,"class":"HasLine"}`.
Use `is_connected_to` to traverse customer connections. `FindDescriptorBatch` and ingestion batch sizes remain supported.

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
of different object types distinct. The same connection selector works for index
creation, lookup and removal on server 1.0.

```python
classes = {"src": "People", "connection": "AppearsIn", "dst": "Photos"}
target = {"src": "entity", "dst": "image"}
utils.create_connection_index(class_name=classes, property_key="id", target=target)
utils.get_indexes(class_name=classes, property_key="id", target=target)
utils.remove_connection_index(class_name=classes, property_key="id", target=target)
```

For a lookup, either dictionary can contain just the fields you want to match.
On server 0.20, pass the connection class name as a string.

In server 1.0 connection filters and metadata definitions, `class.src`,
`class.connection`, and `class.dst` identify the three classes. Endpoint types
go in `target.src` and `target.dst`. To count all `HasLine` connections from
`Order` objects to `LineItem` objects, pass the three names to `FindConnection`.

```json
[{"FindConnection": {
  "class": {"src": "Order", "connection": "HasLine", "dst": "LineItem"},
  "results": {"count": true}
}}]
```

To count the line items for one order, first select that order. Its reference
supplies the source class and type, so the connection query only needs the
connection name and destination class.

```json
[
  {"FindEntity": {"with_class": "Order", "constraints": ["$order_id", "==", 123], "_ref": 1}},
  {"FindConnection": {
    "src": 1,
    "class": {"connection": "HasLine", "dst": "LineItem"},
    "results": {"count": true}
  }}
]
```

A name-only filter can use `with_class` on connection commands or
`connection_class` in a traversal. Supply one class filter per selector.

This aggregate maintains the total value of each order's line items.

```json
[{"CreateAggregate": {
  "class": {"src": "Order", "connection": "HasLine", "dst": "LineItem"},
  "target": {"src": "entity", "dst": "entity"},
  "src_property": "line_total",
  "operation": "sum",
  "dst_expression": "$amount"
}}]
```

Here `dst_expression` reads `amount` from each `LineItem`, and `src_property`
names the `Order` property that receives the sum. To sum a property of the
`HasLine` connection, supply `connection_expression` as the expression.
A `GetAggregates` filter such as `{"class":{"src":"Order"}}`
returns definitions in the same format as the `CreateAggregate` body.

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
