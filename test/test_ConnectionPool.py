import pytest
from aperturedb.ConnectionPool import ConnectionPool


class MockConnector:
    def __init__(self):
        self.queries = []

    def query(self, query: str, blobs: list = [], **kwargs):
        self.queries.append((query, blobs, kwargs))
        return "Success", blobs


def mock_connection_factory():
    return MockConnector()


def test_connection_pool_init():
    pool = ConnectionPool(
        pool_size=5, connection_factory=mock_connection_factory)
    assert pool.total() == 5
    assert pool.available() == 5


def test_connection_pool_get_connection():
    pool = ConnectionPool(
        pool_size=2, connection_factory=mock_connection_factory)

    with pool.get_connection() as conn:
        assert pool.available() == 1
        conn.query("SELECT 1")

    assert pool.available() == 2


def test_connection_pool_query():
    pool = ConnectionPool(
        pool_size=1, connection_factory=mock_connection_factory)

    response, blobs = pool.query("SELECT 2", blobs=[b"data"])
    assert response == "Success"
    assert blobs == [b"data"]
    assert pool.available() == 1


def test_connection_pool_invalid_size():
    with pytest.raises(ValueError):
        ConnectionPool(pool_size=0, connection_factory=mock_connection_factory)


def test_connection_pool_init_failure():
    def failing_factory():
        raise Exception("Failed to connect")

    with pytest.raises(ConnectionError):
        ConnectionPool(pool_size=1, connection_factory=failing_factory)
