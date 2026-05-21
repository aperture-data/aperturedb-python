import threading
import queue
import logging
from contextlib import contextmanager

from aperturedb.CommonLibrary import create_connector

logger = logging.getLogger(__name__)


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
        last_error = None
        for _ in range(pool_size):
            try:
                connection = self._connection_factory()
                if not connection:
                    raise ConnectionError("Failed to create a connection.")
                self._pool.put(connection)
            except Exception as e:
                logger.error(
                    f"Failed to create a connection for the pool: {e}")
                last_error = e
                break

        if self._pool.qsize() < pool_size:
            created = self._pool.qsize()
            # Drain any successfully created connections to prevent leaks
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    if hasattr(conn, 'close'):
                        conn.close()
                except queue.Empty:
                    break
            msg = (
                f"Failed to initialize pool: expected {pool_size} "
                f"connections, got {created}."
            )
            raise ConnectionError(msg) from last_error

    def available(self) -> int:
        """Returns the number of available connections in the pool."""
        return self._pool.qsize()

    def total(self) -> int:
        """Returns the total number of connections in the pool."""
        return self._pool_size

    @contextmanager
    def get_connection(self, timeout=None):
        """
        A context manager to get a connection from the pool.
        This is the recommended way to use a connection.
        It automatically gets a connection and releases it back to the pool.

        Args:
            timeout (float, optional): How long to wait for a connection to become available before raising TimeoutError.

        Note:
            Callers are responsible for the connection's health. If an exception occurs
            within the context manager, the connection is still returned to the pool.

        Usage:
            with pool.get_connection() as conn:
                conn.query(...)
        """
        try:
            connection = self._pool.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(
                f"No connection available in the pool within the specified timeout ({timeout}s).")

        try:
            # Yield the connection for the user to use
            yield connection
        finally:
            # This block is guaranteed to execute, ensuring the connection
            # is always returned to the pool unless an exception occurred.
            self._pool.put(connection)

    def query(self, query, blobs: list = None):
        """
        A convenience method to execute a query directly from the pool.

        This method handles getting a connection, executing the query,
        and returning the connection to the pool.

        Args:
            query: The query string or list to execute.
            blobs (list): A list of blobs to include with the query.

        Returns:
            Response from the executed query.
            Blobs
        """
        if blobs is None:
            blobs = []
        with self.get_connection() as connection:
            return connection.query(query, blobs)

    def close(self):
        """
        Closes all connections in the pool.
        """
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                if hasattr(conn, 'close'):
                    conn.close()
            except queue.Empty:
                break
