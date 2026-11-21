"""Command helpers for the server API selected by its hello version.

Raw queries use the connected server's command language. These helpers let SDK
wrappers choose equivalent commands without inspecting GetStatus.version.
"""
from copy import deepcopy


OBJECT_TYPES = {
    "Entity": "entity", "Blob": "blob", "Image": "image", "Video": "video",
    "Descriptor": "descriptor", "Frame": "frame", "Clip": "clip",
    "BoundingBox": "bounding_box", "Polygon": "polygon", "Connection": "connection",
}


def hello_version(client):
    """Return the negotiated hello version; custom legacy clients default to 1."""
    value = getattr(client, "hello_version", 1)
    return value if type(value) is int else 1


def typed_server(client):
    return hello_version(client) >= 2


def descriptor_selector(client, class_name):
    return {"with_class" if typed_server(client) else "set": class_name}


VECTOR_KINDS = ("flat", "ivf", "hnsw", "acorn")


def vector_index_commands(version, name, dimensions, metric, engines, metadata=None):
    """Build one native index command per engine, or one descriptor-set command."""
    if not isinstance(name, str) or not name or (version >= 2 and name.startswith("_")):
        raise ValueError(
            "A descriptor class must have a nonempty application name")
    if isinstance(dimensions, bool) or int(dimensions) != dimensions or dimensions <= 0:
        raise ValueError("Descriptor dimensions must be a positive integer")
    if version < 2:
        definition = {"name": name, "dimensions": int(dimensions),
                      "metric": metric, "engines": deepcopy(engines)}
        if metadata is not None:
            definition["properties"] = deepcopy(metadata)
        return [{"AddDescriptorSet": definition}]

    if not engines:
        raise ValueError("Select at least one descriptor index kind")
    commands = []
    for kind, settings in engines.items():
        if kind not in VECTOR_KINDS or not isinstance(settings, dict):
            raise ValueError(
                "Invalid descriptor index configuration for " + str(kind))
        if {"dimensions", "metric", "engines"} & settings.keys():
            raise ValueError(
                "Engine settings must not override the vector definition")
        params = {"dimensions": int(dimensions),
                  "metric": metric, **deepcopy(settings)}
        definition = {"target": "descriptor", "class": name, "property": "_blob",
                      "kind": kind, "params": params}
        if metadata is not None:
            definition["metadata"] = deepcopy(metadata)
        commands.append({"CreateIndex": definition})
    return commands


def preferred_vector_index(indexes, engine=None, metric=None):
    """Select the first matching index in the SDK's default engine order."""
    kinds = (engine.lower(),) if engine else VECTOR_KINDS
    for kind in kinds:
        for index in indexes:
            if index["kind"] != kind:
                continue
            metrics = index["params"]["metric"]
            if metric is None or (metric == metrics if isinstance(metrics, str) else metric in metrics):
                return index
    return None


def vector_index(client, class_name, engine=None, metric=None):
    """Read the matching native index with its metrics and metadata."""
    response, _ = client.query([{"GetIndexes": {
        "target": "descriptor", "class": class_name, "property": "_blob",
    }}])
    if not isinstance(response, list) or not response:
        raise ValueError("Cannot read vector index: " + str(response))
    result = response[0]["GetIndexes"]
    if result.get("status") != 0:
        raise ValueError("Cannot read vector index: " + str(result))
    index = preferred_vector_index(result.get("indexes") or [], engine, metric)
    if index is None:
        raise ValueError(
            "No matching vector index for descriptor class " + class_name)
    return index


def connection_selector(class_name, target):
    """Validate an exact connection category for a typed metadata command."""
    if (not isinstance(class_name, dict) or set(class_name) != {"src", "connection", "dst"}
            or not isinstance(target, dict) or set(target) != {"src", "dst"}
            or not all(isinstance(value, str) and value for value in (*class_name.values(), *target.values()))):
        raise ValueError("Supply class_name with src, connection and dst class names "
                         "and target with src and dst endpoint types")
    return {"class": class_name, "target": target}


def schema_for_display(schema):
    """Adapt typed schema arrays to the SDK's display-only graph structure.

    Public get_schema() continues to return the server's original response.
    Type-qualified labels keep identically named application classes distinct.
    """
    if not isinstance(schema.get("nodes"), list):
        return schema
    result = deepcopy(schema)
    entities = {}
    for entry in result.get("nodes") or []:
        item = dict(entry)
        item["_target"] = entry["type"]
        item["_class"] = entry["class"]
        entities[f'{entry["type"]}:{entry["class"]}'] = item
    connections = {}
    for entry in result.get("edges") or []:
        item = dict(entry)
        item["_target"] = "connection"
        classes = entry["class"]
        item["_class"] = classes["connection"]
        item["src"] = f'{entry["target"]["src"]}:{classes["src"]}'
        item["dst"] = f'{entry["target"]["dst"]}:{classes["dst"]}'
        connections.setdefault(classes["connection"], []).append(item)
    result["entities"] = {"returned": len(entities), "classes": entities}
    result["connections"] = {"returned": len(
        connections), "classes": connections}
    return result
