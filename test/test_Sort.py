import pytest
from aperturedb.Sort import Sort, Order

def test_single_sort():
    s = Sort("name", Order.ASCENDING)
    assert s._sort == {"key": "name", "order": "ascending"}

def test_multi_sort():
    s = Sort("name", Order.ASCENDING)
    s.append("age", Order.DESCENDING)
    assert s._sort == [
        {"key": "name", "order": "ascending"},
        {"key": "age", "order": "descending"}
    ]

def test_sort_chaining():
    s = Sort("name", Order.ASCENDING).append("age", Order.DESCENDING).append("score", Order.ASCENDING)
    assert s._sort == [
        {"key": "name", "order": "ascending"},
        {"key": "age", "order": "descending"},
        {"key": "score", "order": "ascending"}
    ]

def test_sort_value_error():
    s = Sort("name", Order.ASCENDING)
    # Intentionally break the parallel arrays to test the ValueError
    s.keys.append("age")
    with pytest.raises(ValueError, match="Number of keys and orders must match."):
        _ = s._sort
