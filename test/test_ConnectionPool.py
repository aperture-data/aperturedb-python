import unittest
import threading
from aperturedb.ConnectionPool import ConnectionPool
from aperturedb.Connector import Connector
try:
    from . import dbinfo
except ImportError:
    import dbinfo


def _make_connector():
    return Connector(
        host=dbinfo.DB_TCP_HOST,
        port=dbinfo.DB_TCP_PORT,
        user=dbinfo.DB_USER,
        password=dbinfo.DB_PASSWORD,
        use_ssl=True,
        ca_cert=dbinfo.CA_CERT,
        verify_hostname=dbinfo.VERIFY_HOSTNAME,
        retry_max_attempts=3,
        retry_interval_seconds=0
    )


class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        pass

    def test_pool_initialization(self):
        pool = ConnectionPool(
            pool_size=3, connection_factory=_make_connector)
        self.assertEqual(pool.total(), 3)
        self.assertEqual(pool.available(), 3)

    def test_pool_borrow_return(self):
        pool = ConnectionPool(
            pool_size=2, connection_factory=_make_connector)
        with pool.get_connection() as conn:
            self.assertEqual(pool.available(), 1)
            # Test a simple query to ensure the connection is real
            response, _ = conn.query([{"GetStatus": {}}])
            self.assertTrue(isinstance(response, list))

        # Should be returned to the pool
        self.assertEqual(pool.available(), 2)

    def test_pool_convenience_query(self):
        pool = ConnectionPool(
            pool_size=1, connection_factory=_make_connector)
        response, blobs = pool.query([{"GetStatus": {}}])
        self.assertTrue(isinstance(response, list))
        self.assertTrue(isinstance(blobs, list))

    def test_pool_concurrency(self):
        pool = ConnectionPool(
            pool_size=5, connection_factory=_make_connector)
        results = []

    def test_pool_concurrency(self):
        pool = ConnectionPool(
            pool_size=5, connection_factory=_make_connector)
        results = []

        def worker():
            try:
                res, _ = pool.query([{"GetStatus": {}}])
                results.append(res)
            except Exception as e:
                results.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 10)
        for r in results:
            self.assertTrue(isinstance(r, list),
                            f"Worker failed with exception: {r}")

    def test_pool_timeout(self):
        pool = ConnectionPool(pool_size=1, connection_factory=_make_connector)
        with pool.get_connection():
            with self.assertRaises(TimeoutError):
                with pool.get_connection(timeout=0.1):
                    pass


if __name__ == '__main__':
    unittest.main()
