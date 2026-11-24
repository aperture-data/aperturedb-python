"""Offline checks for building entity ingestion batches."""
import pandas as pd
import pytest

from aperturedb.EntityDataCSV import EntityDataCSV


@pytest.mark.parametrize("hello_version", [1, 2])
def test_entity_batch_matches_scalar_commands(hello_version):
    frame = pd.DataFrame({
        "EntityClass": ["Person", "Book", "Person"] * 6,
        "constraint_id": range(1, 19),
        "display name": ["Ada", float("nan"), None] * 6,
        "date:published": ["2020-01-01", None, "2021-02-03"] * 6,
        "constraint_date:published": ["2020-01-01", "$value", "2021-02-03"] * 6,
    }, index=pd.RangeIndex(10, 28), dtype=object)
    data = EntityDataCSV(None, df=frame, hello_version=hello_version,
                         strict_response_validation=True)
    expected = [data[i] for i in range(len(data))]
    assert expected[1] == ([{"AddEntity": {
        "class": "Book",
        "properties": {"published": {"_date": None}},
        "if_not_found": ["all", ["$id", "==", 2], ["$published", "==", "$$value"]],
    }}], [])
    for start, stop in [(0, 16), (16, 18), (1, 1)]:
        batch = data[start:stop]
        assert list(batch) == expected[start:stop]
        assert batch.strict_response_validation
        assert batch.hello_version == hello_version


def test_entity_batch_reads_current_rows_in_one_pass(monkeypatch):
    data = EntityDataCSV(None, df=pd.DataFrame({
        "EntityClass": ["Record"] * 24, "constraint_id": range(24),
        "name": [f"Record {i}" for i in range(24)]}))
    batches = []
    itertuples = pd.DataFrame.itertuples

    def record_batch(frame, *args, **kwargs):
        batches.append(list(frame.index))
        return itertuples(frame, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "itertuples", record_batch)
    first = data[:16]
    data.df.loc[8, "name"] = "updated"
    assert list(data[8:24]) == [data[i] for i in range(8, 24)]
    assert first[8][0][0]["AddEntity"]["properties"] == {"name": "Record 8"}
    assert list(data[:2]) == [data[0], data[1]]
    assert batches == [list(range(16)), list(range(8, 24))]


def test_entity_batch_preserves_nullable_property_error():
    data = EntityDataCSV(None, df=pd.DataFrame({
        "EntityClass": ["Record"] * 16,
        "value": pd.array(list(range(15)) + [pd.NA], dtype="Int64")}))
    with pytest.raises(TypeError, match="boolean value of NA is ambiguous"):
        data[:]


def test_entity_batch_preserves_subclass_row_builder():
    class CustomEntityDataCSV(EntityDataCSV):
        def getitem(self, idx):
            commands, blobs = super().getitem(idx)
            commands[0]["AddEntity"]["properties"] = {"position": idx}
            return commands, blobs

    data = CustomEntityDataCSV(None, df=pd.DataFrame({
        "EntityClass": ["Record"] * 17}))
    assert list(data[1:17]) == [data[i] for i in range(1, 17)]


def test_entity_batch_preserves_noncontiguous_row_selection():
    data = EntityDataCSV(None, df=pd.DataFrame({
        "EntityClass": ["Record"] * 32, "id": range(32)},
        index=pd.RangeIndex(5, 69, 2)))
    assert list(data[0:32:2]) == [data[i] for i in range(0, 32, 2)]
    with pytest.raises(KeyError):
        data[:16]
