import pytest
import threading
from unittest.mock import MagicMock
from aperturedb.ConnectionPool import ConnectionPool


def create_mock_connector():
    mock = MagicMock()
    mock.query.return_value = ({"GetNodeInfo": {}}, [])
    return mock


def test_connection_pool_init():
    pool = ConnectionPool(
        pool_size=2, connection_factory=create_mock_connector)
    assert pool.total() == 2
    assert pool.available() == 2


def test_connection_pool_invalid_size():
    with pytest.raises(ValueError):
        ConnectionPool(pool_size=0, connection_factory=create_mock_connector)


def test_get_connection():
    pool = ConnectionPool(
        pool_size=2, connection_factory=create_mock_connector)
    with pool.get_connection() as conn:
        assert conn is not None
        assert pool.available() == 1
    assert pool.available() == 2


def test_query():
    pool = ConnectionPool(
        pool_size=1, connection_factory=create_mock_connector)
    res, blobs = pool.query([{"GetNodeInfo": {}}])
    assert res is not None


def test_threads():
    pool = ConnectionPool(
        pool_size=5, connection_factory=create_mock_connector)

    def worker():
        with pool.get_connection() as conn:
            res, blobs = conn.query([{"GetNodeInfo": {}}])
            assert res is not None

    threads = []
    for i in range(10):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
