"""Command-helper compatibility independent of live server credentials."""
from unittest.mock import MagicMock, patch

import pytest

from aperturedb.DescriptorEngines import descriptor_search
from aperturedb.Descriptors import Descriptors
from aperturedb.ServerCompatibility import descriptor_selector, schema_for_display
from aperturedb.Utils import Utils


def client(version):
    result = MagicMock()
    result.hello_version = version
    result.clone.return_value = result
    result.last_query_ok.return_value = True
    return result


@pytest.mark.parametrize("version,key", [(1, "set"), (2, "with_class")])
def test_descriptor_selector_uses_only_hello(version, key):
    db = client(version)
    assert descriptor_selector(db, "Faces") == {key: "Faces"}
    db.query.assert_not_called()


def test_modern_search_uses_vector_definition_and_explicit_metric():
    db = client(2)
    db.query.return_value = ([{"GetIndexes": {"status": 0, "indexes": [{
        "kind": "hnsw", "params": {"metric": ["CS"]}
    }, {
        "kind": "flat", "params": {"metric": ["L2"]}
    }]}}], [])
    assert descriptor_search(db, "Faces") == {
        "with_class": "Faces", "engine": "Flat", "metric": "L2"}
    assert descriptor_search(db, "Faces", "HNSW", "CS") == {
        "with_class": "Faces", "engine": "HNSW", "metric": "CS"}
    assert all(list(call.args[0][0]) == ["GetIndexes"]
               for call in db.query.call_args_list)


@pytest.mark.parametrize("version,command", [(1, "AddDescriptorSet"), (2, "CreateIndex")])
def test_vector_creation_preserves_metadata_and_engine_settings(version, command):
    utils = Utils(client(version))
    engines = {"hnsw": {"M": 24, "lazy_indexing": True}}
    with patch.object(utils, "execute", return_value=([], [])) as execute:
        assert utils.create_vector_index("Faces", 8, ["L2", "CS"], engines,
                                         {"model": "v1"})
    body = execute.call_args.args[0][0][command]
    if version == 2:
        assert body == {"target": "descriptor", "class": "Faces", "property": "_blob",
                        "kind": "hnsw", "params": {"dimensions": 8, "metric": ["L2", "CS"],
                                                   "M": 24, "lazy_indexing": True},
                        "metadata": {"model": "v1"}}
    else:
        assert body == {"name": "Faces", "dimensions": 8,
                        "metric": ["L2", "CS"], "engines": engines,
                        "properties": {"model": "v1"}}


@pytest.mark.parametrize("version", [1, 2])
def test_kind_and_params_select_one_index(version):
    utils = Utils(client(version))
    with patch.object(utils, "execute") as execute:
        assert utils.create_vector_index(
            "Faces", 8, kind="ivf", params={"nlist": 32})
    [command] = execute.call_args.args[0]
    if version == 2:
        assert command["CreateIndex"]["kind"] == "ivf"
        assert command["CreateIndex"]["params"] == {
            "metric": "CS", "dimensions": 8, "nlist": 32}
    else:
        assert command["AddDescriptorSet"]["engines"] == {"ivf": {"nlist": 32}}


def test_configured_engines_create_separate_indexes_in_one_query():
    utils = Utils(client(2))
    with patch.object(utils, "execute") as execute:
        assert utils.create_vector_index(
            "Faces", 8, engines={"flat": {}, "ivf": {"nprobe": 2}})
    commands = [item["CreateIndex"] for item in execute.call_args.args[0]]
    assert [index["kind"] for index in commands] == ["flat", "ivf"]
    assert "nprobe" not in commands[0]["params"]
    assert commands[1]["params"]["nprobe"] == 2


def test_legacy_named_helper_creates_native_vector_index_on_modern_server():
    utils = Utils(client(2))
    with patch.object(utils, "execute", return_value=([], [])) as execute:
        assert utils.add_descriptorset(
            "Faces", 8, engine=["Flat"], properties={"model": "v1"})
    assert execute.call_args.args[0][0]["CreateIndex"]["metadata"] == {
        "model": "v1"}


def test_modern_descriptor_class_listing_and_count():
    utils = Utils(client(2))
    with patch.object(utils, "get_indexes", return_value=[{"class": "Faces", "kind": "flat"}, {"class": "Faces", "kind": "hnsw"}, {"class": "Text", "kind": "ivf"}]):
        assert utils.get_descriptorset_list() == ["Faces", "Text"]
        assert utils.count_descriptorsets() == 2
    with patch.object(utils, "execute", return_value=([{"FindDescriptor": {"count": 11}}], [])) as run:
        assert utils.count_descriptors_in_set("Faces") == 11
    assert run.call_args.args[0] == [{"FindDescriptor": {
        "with_class": "Faces", "results": {"count": True}}}]


def test_modern_class_removal_is_one_transaction():
    utils = Utils(client(2))
    with patch.object(utils, "get_schema", return_value={"nodes": [{"type": "descriptor", "class": "Faces"}], "edges": []}), \
            patch.object(utils, "get_indexes", return_value=[{
                "target": "descriptor", "class": "Faces", "property": "_blob", "kind": "flat"}]), \
            patch.object(utils, "execute", return_value=([], [])) as run:
        assert utils.remove_descriptorset("Faces")
    assert run.call_args.args[0] == [
        {"DeleteDescriptor": {"with_class": "Faces"}},
        {"RemoveIndex": {"target": "descriptor", "class": "Faces",
                         "property": "_blob", "kind": "flat"}},
        {"RemoveClass": {"target": "descriptor", "class": "Faces"}},
    ]


def test_index_normalization_preserves_new_metadata_progress_and_endpoints():
    definition = {"target": "descriptor", "class": "Faces", "property": "_blob",
                  "kind": "flat", "params": {"dimensions": 8},
                  "metadata": {"model": "v1"}, "progress": {"generation": 3}}
    assert Utils._parse_indexes([definition]) == [definition]
    edge = {"target": {"src_type": "image", "dst_type": "entity"},
            "class": "owns", "property": "id", "kind": "ordered", "params": {},
            "src_class": "Assets", "dst_class": "Customers"}
    assert Utils._parse_indexes([edge]) == [edge]


def test_typed_index_helper_selects_native_object_type():
    utils = Utils(client(2))
    with patch.object(utils, "_has_index", return_value=False), \
            patch.object(utils, "execute", return_value=([], [])) as execute:
        assert utils.create_entity_index("image:Assets", "id")
    assert execute.call_args.args[0][0]["CreateIndex"] == {
        "target": "image", "class": "Assets", "property": "id", "kind": "ordered"}
    legacy = Utils(client(1))
    with patch.object(legacy, "_has_index", return_value=False), \
            patch.object(legacy, "execute", return_value=([], [])) as execute:
        assert legacy.create_entity_index("image:Assets", "id")
    assert execute.call_args.args[0][0]["CreateIndex"]["class"] == "image:Assets"


def test_schema_display_keeps_same_named_classes_distinct_without_mutation():
    schema = {"status": 0, "nodes": [
        {"type": "image", "class": "Customers", "matched": 2, "properties": {}},
        {"type": "entity", "class": "Customers", "matched": 3, "properties": {}},
    ], "edges": [{"class": "owns", "src_type": "entity", "src_class": "Customers",
                  "dst_type": "image", "dst_class": "Customers", "matched": 2}]}
    displayed = schema_for_display(schema)
    assert set(displayed["entities"]["classes"]) == {
        "image:Customers", "entity:Customers"}
    assert displayed["connections"]["classes"]["owns"][0]["dst"] == "image:Customers"
    assert isinstance(schema["nodes"], list)


def test_descriptor_wrapper_sends_modern_search_and_leaves_input_vector_order():
    db = client(2)
    descriptors = Descriptors(db)
    with patch("aperturedb.Descriptors.descriptor_search", return_value={
        "with_class": "Faces", "engine": "Flat", "metric": "CS"}), \
            patch("aperturedb.Descriptors.execute_query", return_value=(
                0, [{"FindDescriptor": {"status": 0, "entities": []}}], [])) as run:
        descriptors.find_similar("Faces", [1.0, 2.0], 3)
    body = run.call_args.args[1][0]["FindDescriptor"]
    assert body["with_class"] == "Faces" and "set" not in body
    assert body["engine"] == "Flat" and body["metric"] == "CS"


def test_connection_metadata_uses_endpoint_object_targets():
    utils = Utils(client(2))
    with pytest.raises(ValueError, match="endpoint types"):
        utils.create_connection_index("owns", "id")
    endpoints = dict(src_type="entity", src_class="People",
                     dst_type="image", dst_class="Assets")
    with patch.object(utils, "execute", return_value=([], [])) as run:
        assert utils.create_connection_index("owns", "id", **endpoints)
        assert utils.remove_connection_index("owns", "id", **endpoints)
    for call in run.call_args_list:
        body = next(iter(call.args[0][0].values()))
        assert body["target"] == {"src_type": "entity", "dst_type": "image"}
        assert body["src_class"] == "People" and body["dst_class"] == "Assets"
        assert "src_type" not in body and "dst_type" not in body


def test_removing_vector_class_removes_endpoint_scoped_dependencies():
    utils = Utils(client(2))
    edge = {"class": "owns", "src_type": "entity", "src_class": "People",
            "dst_type": "descriptor", "dst_class": "Faces", "matched": 1}
    definition = {"target": {"src_type": "entity", "dst_type": "descriptor"},
                  "class": "owns", "src_class": "People", "dst_class": "Faces",
                  "property": "id", "kind": "ordered"}
    with patch.object(utils, "get_schema", return_value={"nodes": [{"type": "descriptor", "class": "Faces"}], "edges": [edge]}), \
            patch.object(utils, "get_indexes", return_value=[definition]), \
            patch.object(utils, "execute", return_value=([], [])) as run:
        assert utils.remove_descriptorset("Faces")
    assert run.call_args.args[0] == [
        {"DeleteDescriptor": {"with_class": "Faces"}},
        {"RemoveIndex": definition},
        {"RemoveClass": {"target": definition["target"], "class": "owns",
                         "src_class": "People", "dst_class": "Faces"}},
        {"RemoveClass": {"target": "descriptor", "class": "Faces"}},
    ]


def test_get_connection_indexes_maps_generic_filter_to_partial_endpoint_target():
    utils = Utils(client(2))
    with patch.object(utils, "execute", return_value=([{"GetIndexes": {"indexes": []}}], [])) as run:
        assert utils.get_indexes(target="connection", class_name="owns") == []
    assert run.call_args.args[0] == [
        {"GetIndexes": {"target": {}, "class": "owns"}}]


def test_missing_descriptor_class_removal_is_idempotent():
    utils = Utils(client(2))
    with patch.object(utils, "get_schema", return_value={"nodes": [], "edges": []}), \
            patch.object(utils, "execute") as run:
        assert utils.remove_descriptorset("Missing")
    run.assert_not_called()


def test_schema_diagram_uses_distinct_safe_node_ids_for_typed_classes():
    utils = Utils(client(2))
    schema = {"status": 0, "nodes": [
        {"type": "entity", "class": "Same", "matched": 1, "properties": {}},
        {"type": "image", "class": "Same", "matched": 1, "properties": {}},
    ], "edges": [{"class": "owns:part", "src_type": "entity", "src_class": "Same",
                  "dst_type": "image", "dst_class": "Same", "matched": 1, "properties": {}}]}
    with patch.object(utils, "get_schema", return_value=schema), \
            patch.object(utils, "get_indexes", return_value=[]):
        graph = utils.visualize_schema()
    assert "entity:Same" in graph.source and "image:Same" in graph.source
    assert "node0:connection0 -> node1" in graph.source


def test_connection_index_display_is_scoped_to_its_endpoint_categories():
    utils = Utils(client(2))
    definitions = [{"target": {"src_type": "entity", "dst_type": "image"},
                    "class": "owns", "property": "id", "kind": "ordered",
                    "src_class": "People", "dst_class": "Assets"}]
    with patch.object(utils, "get_indexes", return_value=definitions) as run:
        lookup = utils._indexed_lookup()
        assert lookup("connection", "owns", "id", src_type="entity", src_class="People",
                      dst_type="image", dst_class="Assets")
        assert not lookup("connection", "owns", "id", src_type="entity", src_class="People",
                          dst_type="video", dst_class="Assets")
    run.assert_called_once()


@pytest.mark.parametrize("remaining,success", [(0, True), (1, False)])
def test_remove_all_native_objects_uses_valid_selectors_atomically(remaining, success):
    utils = Utils(client(2))
    definition = {"target": "descriptor", "class": "Faces",
                  "property": "_blob", "kind": "flat"}
    schema = {"nodes": [{"type": "blob", "class": "Files",
                         "matched": remaining}], "edges": []}

    def validate_deletions(query, **kwargs):
        deletes = {command: body for item in query for command, body in item.items()
                   if command.startswith("Delete")}
        assert set(deletes) == {"Delete" + name for name in (
            "Connection", "Image", "Video", "Blob", "Clip", "BoundingBox",
            "Polygon", "Frame", "Descriptor", "Entity")}
        # Native media delete validation rejects empty commands. Accept a real
        # selector so this regression fails before the fix even with no video.
        for name, body in deletes.items():
            if name != "DeleteConnection":
                assert body.get("constraints") or body.get(
                    "with_class") or body.get("ref"), name
        assert query[-1] == {"RemoveIndex": definition}
        assert kwargs["success_statuses"] == [0, 2]
        return [], []

    with patch.object(utils, "get_indexes", return_value=[definition]), \
            patch.object(utils, "get_schema", return_value=schema), \
            patch.object(utils, "execute", side_effect=validate_deletions) as run:
        assert utils.remove_all_objects() is success
    run.assert_called_once()
