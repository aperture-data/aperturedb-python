import pytest
from unittest.mock import MagicMock
from aperturedb.ConnectionPool import ConnectionPool

def test_connection_pool_init():
    mock_connector = MagicMock()
    mock_factory = MagicMock(return_value=mock_connector)
    
    pool = ConnectionPool(pool_size=5, connection_factory=mock_factory)
    
    assert pool.total() == 5
    assert pool.available() == 5
    assert mock_factory.call_count == 5

def test_connection_pool_invalid_size():
    with pytest.raises(ValueError):
        ConnectionPool(pool_size=0)

def test_connection_pool_get_connection():
    mock_connector = MagicMock()
    mock_factory = MagicMock(return_value=mock_connector)
    
    pool = ConnectionPool(pool_size=2, connection_factory=mock_factory)
    
    with pool.get_connection() as conn:
        assert conn == mock_connector
        assert pool.available() == 1
        
    assert pool.available() == 2

def test_connection_pool_query():
    mock_connector = MagicMock()
    mock_connector.query.return_value = (MagicMock(), [])
    mock_factory = MagicMock(return_value=mock_connector)
    
    pool = ConnectionPool(pool_size=2, connection_factory=mock_factory)
    
    res, blobs = pool.query("Some query", blobs=[])
    
    assert pool.available() == 2
    mock_connector.query.assert_called_once_with("Some query", [], **{})

def test_connection_pool_args():
    mock_connector = MagicMock()
    mock_factory = MagicMock(return_value=mock_connector)
    
    pool = ConnectionPool(pool_size=2, connection_factory=mock_factory, arg1="value1", arg2="value2")
    mock_factory.assert_called_with(arg1="value1", arg2="value2")
