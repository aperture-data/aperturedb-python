import json
from unittest.mock import MagicMock, patch

import pytest

from aperturedb.DescriptorEngines import (accepts_engines_object,
                                          descriptorset_engine, engines_object,
                                          select_engine)
from aperturedb.DescriptorSetDataCSV import DescriptorSetDataCSV
from aperturedb.Descriptors import Descriptors
from aperturedb.Query import Query
from aperturedb.Utils import Utils


def schema_response(add_descriptorset_properties):
    return [{"GetApiSchema": {"status": 0, "schema": {"items": {"properties": {
        "AddDescriptorSet": {"properties": add_descriptorset_properties}}}}}}], []


@pytest.mark.parametrize("engine, expected", [
    ("HNSW", {"hnsw": {}}),
    (["Flat", "HNSW"], {"flat": {}, "hnsw": {}}),
    (["FaissIVFFlat"], {"ivf": {}}),
    ([], {"ivf": {}}),
])
def test_engines_object(engine, expected):
    assert engines_object(engine) == expected


@pytest.mark.parametrize("configured, expected", [
    (["HNSW", "Flat"], "Flat"),
    (["HNSW", "IVF"], "IVF"),
    (["FaissHNSWFlat"], "HNSW"),
    ({"hnsw": {"M": 16}, "ivf": {}}, "IVF"),
    ({"acorn": {"lazy_indexing": False}}, "ACORN"),
    ({"hnsw": None}, None),
    ([], None),
    (None, None),
])
def test_select_engine_reads_both_engine_forms(configured, expected):
    assert select_engine(configured) == expected


def test_descriptorset_engine_reads_the_set_and_tolerates_failures():
    client = MagicMock()
    client.query.return_value = (
        [{"FindDescriptorSet": {"status": 0, "entities": [{"_engines": ["HNSW"]}]}}], [])
    assert descriptorset_engine(client, "faces") == "HNSW"
    client.query.assert_called_once_with(
        [{"FindDescriptorSet": {"with_name": "faces", "engines": True}}])
    client.query.return_value = (
        [{"FindDescriptorSet": {"status": 0, "returned": 0}}], [])
    assert descriptorset_engine(client, "missing") is None
    client.query.side_effect = Exception("connection lost")
    assert descriptorset_engine(client, "faces") is None


def test_accepts_engines_object_reads_the_api_schema_once():
    client = MagicMock()
    client._accepts_engines_object = None
    client.query.return_value = schema_response({"engine": {}, "engines": {}})
    assert accepts_engines_object(client)
    assert accepts_engines_object(client)
    client.query.assert_called_once_with([{"GetApiSchema": {}}])

    older = MagicMock()
    older._accepts_engines_object = None
    older.query.return_value = schema_response({"engine": {}})
    assert not accepts_engines_object(older)
    failed = MagicMock()
    failed._accepts_engines_object = None
    failed.query.side_effect = Exception("unknown command")
    assert not accepts_engines_object(failed)


@pytest.mark.parametrize("accepted", [True, False])
def test_add_descriptorset_uses_the_form_the_server_accepts(accepted):
    utils = Utils(MagicMock())
    with patch("aperturedb.Utils.accepts_engines_object", return_value=accepted), \
            patch.object(utils, "execute", return_value=([], [])) as execute:
        assert utils.add_descriptorset("faces", 4, engine=["Flat", "HNSW"])
    command = execute.call_args.args[0][0]["AddDescriptorSet"]
    if accepted:
        assert command["engines"] == {"flat": {}, "hnsw": {}}
        assert "engine" not in command
    else:
        assert command["engine"] == ["Flat", "HNSW"]
        assert "engines" not in command


@pytest.mark.parametrize("engine, resolved, expected", [
    (None, "HNSW", "HNSW"), ("Flat", "HNSW", "Flat"), (None, None, None)])
def test_find_similar_names_an_engine(engine, resolved, expected):
    descriptors = Descriptors(MagicMock())
    with patch("aperturedb.Descriptors.descriptorset_engine", return_value=resolved), \
            patch("aperturedb.Descriptors.execute_query",
                  return_value=(0, [{"FindDescriptor": {"entities": []}}], [])) as run:
        descriptors.find_similar("faces", [0.0, 1.0], 3, engine=engine)
    command = run.call_args.args[1][0]["FindDescriptor"]
    assert command.get("engine") == expected


def test_query_spec_sends_an_explicit_engine():
    query = Query.spec(set="faces", vector=[0.0], k_neighbors=2, engine="HNSW")
    command = query.query()[0][0]
    params = next(iter(command.values()))
    assert params["k_neighbors"] == 2
    assert params["engine"] == "HNSW"
    params = next(
        iter(Query.spec(set="faces", k_neighbors=2).query()[0][0].values()))
    assert "engine" not in params


def test_descriptorset_csv_accepts_the_engines_object(tmp_path):
    path = tmp_path / "sets.adb.csv"
    engines = json.dumps({"hnsw": {"M": 16}})
    path.write_text('name,dimensions,engines,metric\n'
                    f'faces,4,"{engines.replace(chr(34), chr(34) * 2)}",L2\n')
    query, _ = DescriptorSetDataCSV(str(path)).getitem(0)
    command = query[0]["AddDescriptorSet"]
    assert command["engines"] == {"hnsw": {"M": 16}}
    assert "engine" not in command

    legacy = tmp_path / "legacy.adb.csv"
    legacy.write_text('name,dimensions,engine,metric\nfaces,4,HNSW,L2\n')
    command = DescriptorSetDataCSV(str(legacy)).getitem(0)[
        0][0]["AddDescriptorSet"]
    assert command["engine"] == "HNSW"
