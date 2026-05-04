import unittest
import threading
from aperturedb.ConnectionPool import ConnectionPool


class TestConnectionPool(unittest.TestCase):
    def test_pool_initialization(self):
        pool = ConnectionPool(pool_size=3)
        self.assertEqual(pool.total(), 3)
        self.assertEqual(pool.available(), 3)

    def test_pool_borrow_return(self):
        pool = ConnectionPool(pool_size=2)
        with pool.get_connection() as conn:
            self.assertEqual(pool.available(), 1)
            # Test a simple query to ensure the connection is real
            response, _ = conn.query([{"GetStatus": {}}])
            self.assertTrue(isinstance(response, list))

        # Should be returned to the pool
        self.assertEqual(pool.available(), 2)

    def test_pool_convenience_query(self):
        pool = ConnectionPool(pool_size=1)
        response, blobs = pool.query([{"GetStatus": {}}])
        self.assertTrue(isinstance(response, list))
        self.assertTrue(isinstance(blobs, list))

    def test_pool_concurrency(self):
        pool = ConnectionPool(pool_size=5)
        results = []

        def worker():
            res, _ = pool.query([{"GetStatus": {}}])
            results.append(res)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 10)


if __name__ == '__main__':
    unittest.main()
