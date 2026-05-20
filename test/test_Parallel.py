import logging
import random

from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Subscriptable import Subscriptable

logger = logging.getLogger(__name__)

# Tests for parallel which don't involve data.


class GeneratorWithErrors(Subscriptable):
    def __init__(self, commands_per_query=1, elements=100, error_pct=.5) -> None:
        super().__init__()
        self.commands_per_query = commands_per_query
        self.elements = elements
        self.error_pct = error_pct

    def __len__(self):
        return self.elements

    def getitem(self, subscript):
        query = []
        blobs = []
        for i in range(self.commands_per_query):
            if random.randint(0, 100) <= (self.error_pct * 100):
                query.append({
                    "BadCommand": {
                    }
                })
            else:
                query.append({
                    "FindEntity": {
                        "results": {
                            "count": True
                        }
                    }
                })

        return query, blobs


class TestParallel():
    """
    These check operation of ParallelQuery
    """

    def test_someBadQueries(self, db: Connector):
        """
        Verifies that it handles some queries returning errors
        """
        try:
            elements = 100
            generator = GeneratorWithErrors(elements=elements)
            querier = ParallelQuery(db, dry_run=False)
            querier.query(generator, batchsize=2,
                          numthreads=8,
                          stats=True)
            logger.info(querier.get_succeeded_commands())
            assert querier.get_succeeded_commands() < elements
        except Exception as e:
            print(e)
            print("Failed to renew Session")
            assert False

    def test_allBadQueries(self, db: Connector):
        """
        Verifies that it handles all queries returning errors
        """
        try:
            elements = 100
            generator = GeneratorWithErrors(elements=elements, error_pct=1)
            querier = ParallelQuery(db, dry_run=False)
            querier.query(generator, batchsize=2,
                          numthreads=8,
                          stats=True)
            logger.info(querier.get_succeeded_commands())
            assert querier.get_succeeded_commands() == 0
        except Exception as e:
            print(e)
            print("Failed to renew Session")
            assert False


class GeneratorWithLargeBlobs(Subscriptable):
    def __init__(self, elements=10, blob_size=100) -> None:
        super().__init__()
        self.elements = elements
        self.blob_size = blob_size

    def __len__(self):
        return self.elements

    def getitem(self, subscript):
        query = [{"FindBlob": {}}]
        blobs = [b"0" * self.blob_size]
        return query, blobs


class MockClient:
    def __init__(self):
        from types import SimpleNamespace
        self.config = SimpleNamespace(host="localhost", port=55555, use_ssl=False,
                                      verify_hostname=False, username="admin", password="password")

    def clone(self):
        return self

    def query(self, q, b):
        self.queries.append(q)
        return ([{"FindBlob": {"status": 0}} for _ in range(len(q))], [])

    def last_query_ok(self):
        return True

    def get_last_query_time(self):
        return 0


def test_dynamic_batching():
    db = MockClient()
    db.queries = []

    # 10 elements, 100 bytes each
    generator = GeneratorWithLargeBlobs(10, 100)
    querier = ParallelQuery(db)
    db.queries = []

    # limit to 150 bytes -> should process 1 element per batch despite batchsize=5
    querier.query(generator, batchsize=5, numthreads=1,
                  max_bytes_per_batch=150)

    # It should have succeeded in processing all queries
    assert querier.get_succeeded_queries() == 10
    assert len(db.queries) == 10
    for q in db.queries:
        assert len(q) == 1


def test_dynamic_batching_oversized_item():
    db = MockClient()
    db.queries = []

    # 10 elements, 100 bytes each
    generator = GeneratorWithLargeBlobs(10, 100)
    querier = ParallelQuery(db)
    db.queries = []

    # limit to 50 bytes -> item size (100) > max_bytes (50). Should log warning and process 1 per batch.
    querier.query(generator, batchsize=5, numthreads=1, max_bytes_per_batch=50)

    # It should have succeeded in processing all queries
    assert querier.get_succeeded_queries() == 10
    assert len(db.queries) == 10
    for q in db.queries:
        assert len(q) == 1
