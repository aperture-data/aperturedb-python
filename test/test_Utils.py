from unittest.mock import patch, MagicMock
import json
import pytest
from aperturedb.Utils import Utils


class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True, \
            "Failed to remove all objects"

    def test_remove_all_indexes(self, utils):
        assert utils.remove_all_indexes() == True, \
            "Failed to remove all indexes"

    def test_get_descriptorset_list(self, utils):
        assert utils.get_descriptorset_list() == []

    def test_censor_tokens(self):
        from aperturedb.LoggingUtils import censor_tokens

        # Normal response (no auth)
        response = [{"FindImage": {"status": 0}}]
        assert censor_tokens(response) == response

        # Auth response with tokens
        response = [{
            "Authenticate": {
                "status": 0,
                "session_token": "adbs_1234567890abcdef",
                "refresh_token": "adbr_abcdef1234567890",
                "other_field": "visible"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["Authenticate"]["session_token"] == "adbs_1234...cdef"
        assert censored[0]["Authenticate"]["refresh_token"] == "adbr_abcd...7890"
        assert censored[0]["Authenticate"]["other_field"] == "visible"

        # Short tokens
        response = [{
            "Authenticate": {
                "status": 0,
                "session_token": "adbs_1234",
                "refresh_token": "short"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["Authenticate"]["session_token"] == "adbs_..."
        assert censored[0]["Authenticate"]["refresh_token"] == "..."

        # RefreshToken command
        response = [{
            "RefreshToken": {
                "status": 0,
                "session_token": "adbs_longertokentest"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["RefreshToken"]["session_token"] == "adbs_long...test"


class TestUtilsSummaryNormalization():

    def test_summary_normalization(self):
        # We don't use the 'utils' fixture because it requires a live DB connection
        mock_connector = MagicMock()
        utils = Utils(mock_connector)

        mock_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": {
                        "matched": 10,
                        "properties": {
                            "name": [10, True, "string"]
                        }
                    }
                }
            },
            "connections": {
                "returned": 3,
                "classes": {
                    "Knows": {
                        "matched": 5,
                        "properties": {},
                        "src": "Person",
                        "dst": "Person"
                    },
                    "Likes": {
                        "Likes_1": {
                            "matched": 3,
                            "properties": {},
                            "src": "Person",
                            "dst": "Movie"
                        },
                        "Likes_2": {
                            "matched": 4,
                            "properties": {},
                            "src": "Person",
                            "dst": "Book"
                        }
                    },
                    "Owns": [
                        {
                            "matched": 2,
                            "properties": {},
                            "src": "Person",
                            "dst": "Car"
                        }
                    ]
                }
            }
        }
        mock_status = json.dumps(
            [{"GetStatus": {"version": "1.0", "status": "OK", "info": ""}}])

        with patch.object(utils, 'get_schema', return_value=mock_schema), \
                patch.object(utils, 'status', return_value=mock_status):
            # should not raise
            utils.summary()

    def test_summary_property_object_form(self, capsys):
        mock_connector = MagicMock()
        utils = Utils(mock_connector)

        mock_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": {
                        "matched": 10,
                        "properties": {
                            "name": {"count": 10, "type": "string"},
                            "id": {"count": 10, "type": "number"}
                        }
                    }
                }
            },
            "connections": {
                "returned": 1,
                "classes": {
                    "Knows": [{
                        "matched": 5,
                        "properties": {"since": {"count": 4, "type": "date"}},
                        "src": "Person",
                        "dst": "Person"
                    }]
                }
            }
        }
        mock_status = json.dumps(
            [{"GetStatus": {"version": "1.0", "status": "OK", "info": ""}}])
        indexes = [{"target": "entity", "class": "Person", "property": "name",
                    "kind": "ordered", "params": {}}]

        with patch.object(utils, 'get_schema', return_value=mock_schema), \
                patch.object(utils, 'status', return_value=mock_status), \
                patch.object(utils, 'get_indexes', return_value=indexes) as get_indexes:
            utils.summary()

        get_indexes.assert_called_once()
        lines = capsys.readouterr().out.splitlines()
        name_line = next(l for l in lines if "| name" in l)
        id_line = next(l for l in lines if "| id" in l)
        since_line = next(l for l in lines if "| since" in l)
        assert name_line.startswith("I ")
        assert id_line.startswith("  !")
        assert since_line.startswith("  !") and "4" in since_line


class TestUtilsResponseShapes():

    def test_property_info(self):
        assert Utils._property_info([3, True, "string"]) == (3, True, "string")
        assert Utils._property_info(
            {"count": 3, "type": "string"}) == (3, None, "string")
        assert Utils._property_info(
            {"count": 3, "indexed": False, "type": "number"}) == (3, False, "number")

    def test_parse_indexes_nested(self):
        indexes = {
            "entity": {
                "Person": {
                    "name": [
                        {"kind": "ordered", "params": {"text": "case_sensitive"},
                         "version": 1, "type_versions": []},
                        {"kind": "trigram", "params": {"text": "dual_case"},
                         "version": 1, "type_versions": []}
                    ]
                }
            },
            "connection": {
                "Knows": {"since": [{"kind": "ordered", "params": {}}]}
            }
        }
        assert Utils._parse_indexes(indexes) == [
            {"target": "entity", "class": "Person", "property": "name",
             "kind": "ordered", "params": {"text": "case_sensitive"}},
            {"target": "entity", "class": "Person", "property": "name",
             "kind": "trigram", "params": {"text": "dual_case"}},
            {"target": "connection", "class": "Knows", "property": "since",
             "kind": "ordered", "params": {}},
        ]
        assert Utils._parse_indexes({}) == []

    def test_parse_indexes_flat(self):
        indexes = [{"index_type": "entity", "class": "Person",
                    "property_key": "name", "kind": "ordered", "params": {}}]
        assert Utils._parse_indexes(indexes) == [
            {"target": "entity", "class": "Person", "property": "name",
             "kind": "ordered", "params": {}}]

    def test_get_indexed_props(self):
        utils = Utils(MagicMock())
        indexes = [
            {"target": "entity", "class": "_Image", "property": "a",
             "kind": "ordered", "params": {}},
            {"target": "entity", "class": "_Image", "property": "a",
             "kind": "trigram", "params": {}},
            {"target": "entity", "class": "_Image", "property": "b",
             "kind": "ordered", "params": {}},
        ]
        with patch.object(utils, 'get_indexes', return_value=indexes) as get_indexes:
            assert utils.get_indexed_props("_Image") == ["a", "b"]
        get_indexes.assert_called_once_with(
            target="entity", class_name="_Image")


class TestUtilsIndexCleanup():

    def test_remove_all_indexes_preserves_only_builtin_definitions(self):
        utils = Utils(MagicMock())
        builtin = {"target": "entity", "class": "_DescriptorSet",
                   "property": "_name", "kind": "ordered"}
        custom = {**builtin, "kind": "trigram"}
        connection = {**builtin, "target": "connection"}
        indexes = [builtin, custom, connection]

        def execute(query):
            for command in query:
                indexes.remove(command["RemoveIndex"])
            return [], []

        with patch.object(utils, 'get_indexes', side_effect=lambda: list(indexes)), \
                patch.object(utils, 'execute', side_effect=execute) as query:
            assert utils.remove_all_indexes()

        assert indexes == [builtin]
        query.assert_called_once_with([
            {"RemoveIndex": custom}, {"RemoveIndex": connection}])

    def test_cleanup_detects_remaining_custom_index(self):
        utils = Utils(MagicMock())
        index = {"target": "entity", "class": "_DescriptorSet",
                 "property": "_name", "kind": "trigram"}
        with patch.object(utils, 'get_indexes', return_value=[index]), \
                patch.object(utils, 'execute', return_value=([], [])):
            assert not utils.remove_all_indexes()

    def test_descriptor_cleanup_removes_custom_kind(self):
        utils = Utils(MagicMock())
        builtin = {"target": "entity", "class": "_Descriptor",
                   "property": "_set_name", "kind": "ordered"}
        custom = {**builtin, "kind": "trigram"}
        with patch.object(utils, 'get_indexes', return_value=[builtin, custom]), \
                patch.object(utils, '_remove_index', return_value=True) as remove, \
                patch.object(utils, 'remove_connections', return_value=True), \
                patch.object(utils, 'remove_entities', return_value=True), \
                patch.object(utils, 'get_descriptorset_list', return_value=[]):
            utils.remove_all_descriptorsets()
        remove.assert_called_once_with(
            "entity", "_Descriptor", "_set_name", "trigram")


@pytest.mark.parametrize("property_form", ["array", "object"])
@pytest.mark.parametrize("connection_form", ["single", "list", "mapping"])
@pytest.mark.parametrize("consumer", ["summary", "visualize_schema"])
def test_schema_consumers_support_both_property_forms(
        property_form, connection_form, consumer, capsys):
    if consumer == "visualize_schema":
        pytest.importorskip("graphviz")
    utils = Utils(MagicMock())
    if property_form == "array":
        name = [10, True, "string"]
        since = [4, False, "date"]
    else:
        name = {"count": 10, "type": "string"}
        since = {"count": 4, "type": "date"}
    connection = {"matched": 5, "src": "Person", "dst": "Person",
                  "properties": {"since": since}}
    if connection_form == "list":
        connection = [connection]
    elif connection_form == "mapping":
        connection = {"Knows_1": connection}
    schema = {
        "entities": {"returned": 1, "classes": {
            "Person": {"matched": 10, "properties": {"name": name}}}},
        "connections": {"returned": 1, "classes": {"Knows": connection}},
    }
    requests = []

    def execute(query):
        requests.append(query)
        if query == [{"GetSchema": {}}]:
            return [{"GetSchema": schema}], []
        assert query == [{"GetIndexes": {}}]
        return [{"GetIndexes": {"indexes": {"entity": {"Person": {
            "name": [{"kind": "ordered", "params": {}}]}}}}}], []

    status = json.dumps([{"GetStatus": {
        "version": "0.19.14", "status": 0, "info": ""}}])
    with patch.object(utils, 'execute', side_effect=execute), \
            patch.object(utils, 'status', return_value=status):
        result = getattr(utils, consumer)()

    if consumer == "summary":
        lines = capsys.readouterr().out.splitlines()
        assert next(line for line in lines if "| name" in line).startswith("I ")
        since_line = next(line for line in lines if "| since" in line)
        assert since_line.startswith("  !") and "(80%)" in since_line
    else:
        assert "Indexed, string" in result.source
        assert "Unindexed, date" in result.source
        assert "Person:Knows -> Person" in result.source
    assert requests.count([{"GetSchema": {}}]) == 1
    assert requests.count([{"GetIndexes": {}}]) == (property_form == "object")


@pytest.mark.parametrize("target", ["entity", "connection"])
def test_index_requests_use_current_selectors_and_explicit_kind(target):
    utils = Utils(MagicMock())
    params = {"target": target, "class": "Person",
              "property": "name", "kind": "ordered"}
    with patch.object(utils, 'execute', return_value=([], [])) as execute:
        assert getattr(utils, f"create_{target}_index")("Person", "name")
        assert getattr(utils, f"remove_{target}_index")("Person", "name")
    assert [call.args[0] for call in execute.call_args_list] == [
        [{"CreateIndex": params}], [{"RemoveIndex": params}]]


@pytest.mark.parametrize("method, command", [
    ("count_images", "FindImage"),
    ("count_bboxes", "FindBoundingBox"),
    ("count_entities", "FindEntity"),
    ("count_connections", "FindConnection"),
])
def test_count_requests_use_inequality(method, command):
    utils = Utils(MagicMock())
    with patch.object(utils, 'execute', return_value=(
            [{command: {"count": 3}}], [])) as execute:
        kwargs = {"constraints": {"age": ["!=", 20]}}
        if method in ("count_entities", "count_connections"):
            kwargs["connections_class" if method ==
                   "count_connections" else "entity_class"] = "Person"
        assert getattr(utils, method)(**kwargs) == 3
    call = execute.call_args
    query = call.kwargs.get("query", call.args[0] if call.args else None)
    assert query[0][command]["constraints"] == ["$age", "!=", 20]
