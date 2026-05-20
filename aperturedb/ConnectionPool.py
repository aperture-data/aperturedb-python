import threading
import queue
from contextlib import contextmanager

from aperturedb.CommonLibrary import create_connector


class ConnectionPool:
    """
    A thread-safe connection pool for aperturedb.Connector.

    This pool manages a fixed number of Connector instances, allowing multiple
    threads to safely execute queries by borrowing and returning connections.
    """

    def __init__(self, pool_size: int = 10, connection_factory=create_connector):
        """
        Initializes the connection pool.

        Args:
            pool_size (int): The number of connections to keep in the pool.
            connection_factory (callable): A factory function to create new connections.
        """
        if pool_size <= 0:
            raise ValueError("Pool size must be greater than 0.")

        self._pool_size = pool_size
        self._connection_factory = connection_factory
        # A thread-safe queue to hold the available connections
        self._pool = queue.Queue(maxsize=pool_size)

        # Pre-populate the pool with connections
        for _ in range(pool_size):
            try:
                connection = self._connection_factory()
                if not connection:
                    raise ConnectionError("Failed to create a connection.")
                self._pool.put(connection)
            except Exception as e:
                raise ConnectionError(f"Failed to create a connection for the pool: {e}") from e

    def available(self) -> int:
        """Returns the number of available connections in the pool."""
        return self._pool.qsize()

    def total(self) -> int:
        """Returns the total number of connections in the pool."""
        return self._pool_size

    @contextmanager
    def get_connection(self):
        """
        A context manager to get a connection from the pool.
        This is the recommended way to use a connection.
        It automatically gets a connection and releases it back to the pool.

        Usage:
            with pool.get_connection() as conn:
                conn.query(...)
        """
        # The get() call will block until a connection is available.
        connection = self._pool.get()
        try:
            # Yield the connection for the user to use
            yield connection
        finally:
            # This block is guaranteed to execute, ensuring the connection
            # is always returned to the pool.
            self._pool.put(connection)

    def query(self, query, blobs=None):
        """
        A convenience method to execute a query directly from the pool.

        This method handles getting a connection, executing the query,
        and returning the connection to the pool.

        Args:
            query: The query to execute (string or JSON object).
            blobs (list): A list of blobs to include with the query.

        Returns:
            Response from the executed query.
            Blobs
        """
        if blobs is None:
            blobs = []
            
        with self.get_connection() as connection:
            return connection.query(query, blobs=blobs)
