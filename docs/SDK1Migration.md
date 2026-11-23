---
slug: /python_sdk/compatibility/1.0.0
sidebar_label: Connections and queries
sidebar_position: 1
---

# Connections, queries and streaming

The default connector uses native TCP with TLS. It requires an endpoint that
accepts hello version 2. To connect through HTTPS, use `ConnectorRest` or set
`use_rest=True` in your connection profile and select the HTTP gateway port.
The HTTP client negotiates the response format with the gateway.

## Buffered queries

`client.query(commands, blobs)` returns `(json_response, output_blobs)` after
reading the complete response. The SDK can retry connection establishment. It
can also renew a session and retry a query if authentication failed before any
commands ran. If the connection fails after transmission, inspect committed
effects before retrying a write. The SDK leaves that retry to the caller.
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

JSON metadata is available immediately. Each output blob is read when the
application advances the iterator. `blob.read()` collects one blob, while `blob.iter_chunks()` bounds
application buffering. Finish each blob before requesting another. Exhaust the
blob iterator, including for HTTP responses with no output blobs, to validate
complete framing and the closing multipart boundary.

Exiting the context with unread data or receiving an incomplete blob closes the
connection. Clone the connector for concurrent work while a response is active.
Late storage or encoding failures raise `ConnectionError`. Native responses and
multipart HTTP responses support incremental delivery. If the gateway returns
JSON with base64-encoded blobs, the HTTP client buffers the response before
exposing the same iterator.
Some encoded media must materialize one object to determine its byte length.

## Classes, descriptors and helpers

Raw queries use the connected server's command API. When writing queries for
typed application classes, specify the class on additions, use array-expression
constraints, and put `limit` and `sort` at the command level.
Owner selectors use `image_ref`/`video_ref` when finding regions or video parts,
and `bounding_box_ref`/`polygon_ref`/`frame_ref`/`clip_ref` for their parents.
To create a customer connection, supply its name as a string, for
example `{"AddConnection":{"class":"HasLine","src":1,"dst":2}}`. The referenced
objects determine the endpoint classes and types. Inline `connect` also takes a
string connection name and a reference, for example `{"ref":2,"class":"HasLine"}`.
Use `is_connected_to` to traverse customer connections, `FindDescriptorBatch`
for batch searches, and the loader's batch size to group insertions.

The SDK helpers select the appropriate command forms for the connected server.

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

The helpers use a descriptor class and native vector index when the server supports
them, or a descriptor set on endpoints that use sets. The `add_descriptorset`,
listing, counting and removal helpers select the corresponding representation.
For a native index, pass properties in `metadata` and select the class with
`FindDescriptor.with_class`. When multiple index configurations exist, pass the
engine and metric to select one. Native index results include metadata, progress
and endpoint selectors.

`Utils.get_schema()` returns the server's schema representation, including the
`nodes` and `edges` arrays on typed-schema endpoints. SDK schema summaries,
diagrams and SPARQL handle both schema representations. Type-qualified selectors
such as `image:Photos` distinguish identically named classes of different object
types. Use the same grouped selector to create, look up and remove a connection
index on endpoints that support grouped connection selectors.

```python
classes = {"src": "People", "connection": "AppearsIn", "dst": "Photos"}
target = {"src": "entity", "dst": "image"}
utils.create_connection_index(class_name=classes, property_key="id", target=target)
utils.get_indexes(class_name=classes, property_key="id", target=target)
utils.remove_connection_index(class_name=classes, property_key="id", target=target)
```

For a lookup, either dictionary can contain just the fields you want to match.
If the endpoint accepts only a name for a connection index, pass the connection
class as a string.

In grouped connection filters and metadata definitions, `class.src`,
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

For typed media, pass the application class to the CSV loader.

```python
from aperturedb.ImageDataCSV import ImageDataCSV
from aperturedb.ParallelLoader import ParallelLoader

data = ImageDataCSV("images.csv", class_name="Photos")
ParallelLoader(client).ingest(data, batchsize=32)
```

Parallel loaders select the connected server’s command format before index
setup and iteration, including sliced CSV inputs and Dask workers. For native
vector indexes, descriptor CSV `set` values name the descriptor class.
Descriptor-set CSV rows define the indexes. The loader rejects graph conditions
and references on index definitions. Typed connection CSV headers use selectors
such as `entity:People@id` and `image:Photos@id`.

Pass `client=client` when generating native model queries outside wrappers.
Model application classes use their Python model names, with an optional
`class_name` override. Specialized query wrappers honor `with_class`, and
generic builders accept type-qualified selectors. Custom `generate_query()`
implementations must emit commands accepted by the connected server.

PyTorch and TensorFlow dataset wrappers select the paging mode for the
endpoint. With `offset`/`limit` paging, the query must be read-only and
`batch_size` controls each client page. Keep the selected data fixed while
iterating because writes can move records between pages. Endpoints with cached
paging use the server's cached result.
