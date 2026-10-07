import pytest

from aperturedb.Constraints import Constraints, literal, to_expression


@pytest.mark.parametrize("constraints, expected", [
    ({"any": {"age": [">=", 20, "<=", 90]}},
     ["all", ["$age", ">=", 20], ["$age", "<=", 90]]),
    ({"any": {"age": [">=", 20, "<=", 90], "name": ["==", "x"]}},
     ["any", ["all", ["$age", ">=", 20], ["$age", "<=", 90]],
      ["$name", "==", "x"]]),
    ({"age": [">=", 20, "<=", 90], "name": ["==", "x"]},
     ["all", ["$age", ">=", 20], ["$age", "<=", 90], ["$name", "==", "x"]]),
    ({"any": {"all": {"age": [">=", 20], "active": ["==", True]},
              "name": ["==", "x"]}},
     ["any", ["all", ["$age", ">=", 20], ["$active", "==", True]],
      ["$name", "==", "x"]]),
    ({}, None),
    ({"all": {}}, None),
])
def test_legacy_constraint_conversion(constraints, expected):
    assert to_expression(constraints) == expected


def test_expression_input_is_preserved():
    expression = ["all", ["$age", ">", 20], ["$usage", "<", "$limit"]]
    assert to_expression(expression) is expression


def test_constraint_literals_remain_literal():
    values = ["$name", "$$name", {"_date": "2026-10-07"}, ["$nested"]]
    assert literal(values) == {"array": [
        "$$name", "$$$name", {"to_date": "2026-10-07"}, {"array": ["$$nested"]}
    ]}
    assert Constraints().is_in("value", values).constraints == [
        "all", ["$value", "in", literal(values)]]


def test_query_uses_command_level_controls():
    from aperturedb.Query import Query
    from aperturedb.Sort import Order, Sort

    commands, blobs = Query.spec(
        with_class="Person", limit=5, sort=Sort("age", Order.ASCENDING),
        group_by_src=True, constraints=Constraints().greater("age", 20)).query()
    command = commands[0]["FindEntity"]
    assert command["limit"] == 5
    assert command["sort"] == {"key": "age", "order": "ascending"}
    assert command["group_by_source"] is True
    assert command["constraints"] == ["all", ["$age", ">", 20]]
    assert command["results"] == {"all_properties": True}
    assert blobs == []


def test_csv_loader_emits_expression_constraints():
    import pandas as pd
    from aperturedb.EntityDataCSV import EntityDataCSV

    data = EntityDataCSV("unused.csv", df=pd.DataFrame({
        "EntityClass": ["Person"], "name": ["$name"],
        "constraint_name": ["$name"]}))
    commands, blobs = data[0]
    assert commands[0]["AddEntity"]["if_not_found"] == [
        "$name", "==", "$$name"]
    assert blobs == []


def test_json_object_literals_are_not_interpreted_as_functions():
    value = {"lower": "$name", "nested": ["$value", {"_date": "literal"}]}
    assert literal(value) == {"json": value}
    assert to_expression({"payload": ["==", value]}) == [
        "$payload", "==", {"json": value}]
