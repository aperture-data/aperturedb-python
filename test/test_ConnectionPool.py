import pytest
from unittest.mock import MagicMock
from aperturedb.ConnectionPool import ConnectionPool

def test_connection_pool_initialization():
    mock_factory = MagicMock()
    mock_connection = MagicMock()
    mock_factory.return_value = mock_connection
    
    pool = ConnectionPool(pool_size=3, connection_factory=mock_factory)
    
    assert pool.total() == 3
    assert pool.available() == 3
    assert mock_factory.call_count == 3

def test_get_connection():
    mock_factory = MagicMock()
    mock_connection = MagicMock()
    mock_factory.return_value = mock_connection
    
    pool = ConnectionPool(pool_size=1, connection_factory=mock_factory)
    
    assert pool.available() == 1
    
    with pool.get_connection() as conn:
        assert pool.available() == 0
        assert conn == mock_connection
        
    assert pool.available() == 1

def test_query():
    mock_factory = MagicMock()
    mock_connection = MagicMock()
    mock_connection.query.return_value = ("response", [])
    mock_factory.return_value = mock_connection
    
    pool = ConnectionPool(pool_size=1, connection_factory=mock_factory)
    
    response, blobs = pool.query("some query", [1, 2, 3], arg=True)
    
    assert response == "response"
    assert blobs == []
    mock_connection.query.assert_called_once_with("some query", [1, 2, 3], arg=True)
