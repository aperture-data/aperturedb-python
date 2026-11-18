"""Typed-schema and URI checks without external data or model downloads."""
from aperturedb.Constraints import predicate
from aperturedb.SPARQL import SPARQL
from rdflib import URIRef
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("rdflib")


def adapter(version=2):
    client = MagicMock()
    client.hello_version = version
    schema = {"nodes": [
        {"type": "entity", "class": "Shared",
            "matched": 1, "properties": {"name": {}}},
        {"type": "image", "class": "Shared",
            "matched": 1, "properties": {"name": {}}},
        {"type": "image", "class": "Other", "matched": 1, "properties": {}},
        {"type": "descriptor", "class": "Faces",
            "matched": 1, "properties": {"label": {}}},
    ], "edges": [{"class": "links", "src_type": "entity", "src_class": "Shared",
                  "dst_type": "image", "dst_class": "Shared", "matched": 1}]}
    if version == 1:
        schema = {"entities": {"classes": {"_Image": {"properties": {"name": {}}}}},
                  "connections": None}
    with patch("aperturedb.SPARQL.Utils") as utils:
        utils.return_value.get_schema.return_value = schema
        return SPARQL(client, debug=True), client


def test_typed_schema_preserves_object_types_and_legal_distinct_prefixes():
    sparql, _ = adapter()
    entity = sparql._make_safe_prefix("entity:Shared")
    image = sparql._make_safe_prefix("image:Shared")
    assert entity != image
    assert sparql._make_command_name(entity) == "FindEntity"
    assert sparql._make_command_name(image) == "FindImage"
    assert sparql._class_selector(image) == {"with_class": "Shared"}
    assert sparql._make_safe_prefix("image:shared") == image
    assert sparql._make_safe_prefix(
        "image:A-B") != sparql._make_safe_prefix("image:A_2d_B")
    assert all(":" not in prefix for prefix in sparql.namespaces)
    assert sparql.properties[sparql.namespaces["p"] +
                             "name"] == {entity, image}
    assert sparql.connections[sparql.namespaces["c"] +
                              "links"] == ({entity}, {image})


@pytest.mark.parametrize("version,type_name,selector", [
    (1, "_Image", {}), (2, "image:Shared", {"with_class": "Shared"}),
])
def test_blob_retrieval_accepts_uri_objects_full_strings_and_class_selectors(version, type_name, selector):
    sparql, client = adapter(version)
    kind = sparql._make_safe_prefix(type_name)
    uri = URIRef(sparql._make_uri("o", kind + "/42"))
    client.query.return_value = ([{"FindImage": {"status": 0}}], [b"image"])
    assert sparql.get_blob(uri) == b"image"
    assert sparql.get_blob(str(uri)) == b"image"
    assert sparql.get_blob(uri, type="adb_Image") == b"image"
    assert client.query.call_args.args[0] == [{"FindImage": {
        **selector, "constraints": predicate("_uniqueid", "==", "42"), "blobs": True}}]


def test_blob_list_preserves_order_across_classes_of_the_same_object_type():
    sparql, client = adapter()
    first = URIRef(sparql._make_uri(
        "o", sparql._make_safe_prefix("image:Shared") + "/1"))
    second = URIRef(sparql._make_uri(
        "o", sparql._make_safe_prefix("image:Other") + "/2"))
    client.query.return_value = ([{"FindImage": {"entities": [
        {"_uniqueid": "2", "_blob_index": 0}, {"_uniqueid": "1", "_blob_index": 1}]}}],
        [b"second", b"first"])
    assert sparql.get_blobs([first, second], type="adb_Image") == [
        b"first", b"second"]
    assert "with_class" not in client.query.call_args.args[0][0]["FindImage"]
    assert sparql.get_blobs([]) == []
    entity = URIRef(sparql._make_uri(
        "o", sparql._make_safe_prefix("entity:Shared") + "/3"))
    with pytest.raises(AssertionError, match="Cannot get blob"):
        sparql.get_blob(entity)


def test_knn_type_is_the_selected_descriptor_class_and_search_is_scoped():
    sparql, client = adapter()
    response = [{"FindDescriptor": {"entities": [
        {"_uniqueid": "42", "label": "near", "_distance": 0}]}}]
    with patch("aperturedb.SPARQL.descriptor_search", return_value={
            "with_class": "Faces", "engine": "Flat", "metric": "L2"}), \
            patch("aperturedb.SPARQL.execute_query", return_value=(0, response, [])) as run:
        rows = list(sparql.query('''SELECT ?d ?label ?distance WHERE {
          ?d knn:similarTo [knn:set "Faces"; knn:k_neighbors 2; knn:engine "Flat";
              knn:metric "L2"; knn:vector "[1,0]"; knn:distance ?distance]; p:label ?label .
        }'''))
    assert len(rows) == 1
    assert str(rows[0][1]) == "near"
    body = run.call_args.args[1][0]["FindDescriptor"]
    assert body["with_class"] == "Faces" and "set" not in body
    assert body["metric"] == "L2" and body["engine"] == "Flat"
    assert sparql._deduce_type(
        rows[0][0]) == sparql._make_safe_prefix("descriptor:Faces")


@pytest.mark.parametrize('rdf_type,command,kind', [
    ('Shared', 'FindEntity', 'entity'),
    ('adb_Image', 'FindImage', 'image'),
])
def test_legacy_rdf_type_names_select_the_equivalent_typed_category(rdf_type, command, kind):
    sparql, _ = adapter()
    response = [
        {command: {'entities': [{'_uniqueid': '42', 'name': 'selected'}]}}]
    with patch('aperturedb.SPARQL.execute_query', return_value=(0, response, [])) as run:
        rows = list(sparql.query(
            f'SELECT ?object ?name WHERE {{ ?object a t:{rdf_type}; p:name ?name . }}'))
    assert len(rows) == 1
    body = run.call_args.args[1][0][command]
    assert body['with_class'] == 'Shared'
    assert sparql._deduce_type(
        rows[0][0]) == sparql._make_safe_prefix(kind + ':Shared')
