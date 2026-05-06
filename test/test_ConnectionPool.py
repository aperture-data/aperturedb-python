import unittest
import threading
from aperturedb.ConnectionPool import ConnectionPool
from aperturedb.CommonLibrary import create_connector


class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        import time
        time.sleep(1)  # Give server time to breathe between tests

    def test_pool_initialization(self):
        pool = ConnectionPool(
            pool_size=3, connection_factory=lambda: create_connector())
        self.assertEqual(pool.total(), 3)
        self.assertEqual(pool.available(), 3)

    def test_pool_borrow_return(self):
        pool = ConnectionPool(
            pool_size=2, connection_factory=lambda: create_connector())
        with pool.get_connection() as conn:
            self.assertEqual(pool.available(), 1)
            # Test a simple query to ensure the connection is real
            response, _ = conn.query([{"GetStatus": {}}])
            self.assertTrue(isinstance(response, list))

        # Should be returned to the pool
        self.assertEqual(pool.available(), 2)

    def test_pool_convenience_query(self):
        pool = ConnectionPool(
            pool_size=1, connection_factory=lambda: create_connector())
        response, blobs = pool.query([{"GetStatus": {}}])
        self.assertTrue(isinstance(response, list))
        self.assertTrue(isinstance(blobs, list))

    def test_pool_concurrency(self):
        pool = ConnectionPool(
            pool_size=5, connection_factory=lambda: create_connector())
        results = []

        def worker():
            try:
                res, _ = pool.query([{"GetStatus": {}}])
                results.append(res)
            except Exception as e:
                print("Error in worker thread", e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 10)


if __name__ == '__main__':
    unittest.main()
