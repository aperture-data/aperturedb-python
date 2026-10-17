import logging
import random
import pytest

from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer

logger = logging.getLogger(__name__)


class DummyTransformer(Transformer):
    def __init__(self, generator, client=None):
        super().__init__(generator, client=client)
        if client is None:
            pytest.fail("Client was not passed to transformer!")

    def getitem(self, idx):
        query, blobs = self.data[idx]
        return query, blobs

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
            if random.random() < self.error_pct:
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
            raise

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
            raise

    def test_dictResponseHandling(self):
        """
        Verifies that it handles a dict response from a failing server properly.
        Guards against regression to "unhashable type: 'slice'" when r is a dict.
        """
        from unittest.mock import MagicMock
        try:
            elements = 10
            generator = GeneratorWithErrors(elements=elements)
            db = MagicMock(spec=Connector)
            db.clone.return_value = db
            db.config = "mock_config"
            db.query.return_value = ([{"GetSchema": {"status": 0}}], [])
            querier = ParallelQuery(db, dry_run=False)

            # Now set the mock for the actual queries
            db.query.return_value = (
                {"status": 3, "error_msg": "mock error"}, [])
            db.last_query_ok.return_value = True
            db.get_last_query_time.return_value = 1.0

            querier.query(generator, batchsize=2, numthreads=1, stats=True)

            # Since all queries got status 3, no commands or queries succeeded.
            assert querier.get_succeeded_commands() == 0
            assert querier.get_succeeded_queries() == 0
            # Ensure stats were recorded properly
            assert len(querier.actual_stats) > 0
        except Exception as e:
            print(e)
            raise

    def test_transformers(self, db: Connector):
        """
        Verifies that transformers are correctly applied.
        """
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)

        loader = ParallelLoader(db)
        loader.ingest(generator, batchsize=2, numthreads=2,
                      stats=False, transformers=[DummyTransformer])

        assert loader.get_succeeded_queries() > 0

    def test_transformers_rejects_dask(self, db: Connector):
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)
        generator.use_dask = True

        loader = ParallelLoader(db)
        with pytest.raises(ValueError, match="Transformers cannot be used with Dask"):
            loader.ingest(generator, batchsize=2, numthreads=2,
                          stats=False, transformers=[DummyTransformer])

        # Test manual wrapping also gets rejected
        transformer = DummyTransformer(generator, client=db)
        with pytest.raises(ValueError, match="Transformers cannot be used with Dask"):
            loader.ingest(transformer, batchsize=2, numthreads=2, stats=False)

    def test_transformers_equivalence(self, db: Connector):
        """
        Verifies that using transformers parameter is equivalent to manual wrapping.
        """
        elements = 10

        # Manual wrapping
        generator1 = GeneratorWithErrors(elements=elements, error_pct=0)
        transformer1 = DummyTransformer(generator1, client=db)
        loader1 = ParallelLoader(db)
        loader1.ingest(transformer1, batchsize=2, numthreads=2, stats=False)

        # transformers parameter
        generator2 = GeneratorWithErrors(elements=elements, error_pct=0)
        loader2 = ParallelLoader(db)
        loader2.ingest(generator2, batchsize=2, numthreads=2,
                       stats=False, transformers=[DummyTransformer])

        assert loader1.get_succeeded_queries() == loader2.get_succeeded_queries()
        assert loader1.get_succeeded_queries() > 0

    def test_query_transformers(self, db: Connector):
        """
        Verifies that transformers are correctly applied when calling ParallelQuery.query().
        """
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)

        querier = ParallelQuery(db)
        querier.query(generator, batchsize=2, numthreads=2,
                      stats=False, transformers=[DummyTransformer])

        assert querier.get_succeeded_queries() > 0

    def test_transformers_single_and_invalid(self, db: Connector):
        """
        Verifies that passing a single transformer works and invalid inputs raise TypeError.
        """
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)
        loader = ParallelLoader(db)

        # Single transformer
        loader.ingest(generator, batchsize=2, numthreads=2,
                      stats=False, transformers=DummyTransformer)
        assert loader.get_succeeded_queries() > 0

        # Invalid input
        with pytest.raises(TypeError, match="must be a subclass of Transformer or a callable"):
            loader.ingest(generator, batchsize=2, numthreads=2,
                          stats=False, transformers="invalid_transformer")

    def test_parallel_query_worker_closes_connection(self, db, monkeypatch):
        from aperturedb.QueryGenerator import QueryGenerator
        import threading

        class MockQueryGenerator(QueryGenerator):
            def __len__(self):
                return 2

            def getitem(self, idx):
                return [{"FindImage": {}}], []

        pq = ParallelQuery(db)

        closed_count = [0]
        original_clone = pq.client.clone

        def mock_clone():
            cloned = original_clone()
            original_close = cloned.close

            def mock_close():
                closed_count[0] += 1
                original_close()
            cloned.close = mock_close
            return cloned
        monkeypatch.setattr(pq.client, "clone", mock_clone)

        original_do_batch = pq.do_batch

        def mock_do_batch(client, batch_start, data):
            if batch_start == 1:
                raise Exception("Simulated do_batch exception")
            original_do_batch(client, batch_start, data)
        monkeypatch.setattr(pq, "do_batch", mock_do_batch)

        # Test exception in do_batch
        pq.query(MockQueryGenerator(), batchsize=1, numthreads=1)
        # Should close even if exception occurred in do_batch
        assert closed_count[0] == 1

        # Test early exit when run_event is cleared
        closed_count[0] = 0
        run_event = threading.Event()
        run_event.clear()  # Not set, so worker breaks immediately

        # worker signature: worker(self, thid: int, generator, start: int, end: int, run_event)
        pq.worker(0, MockQueryGenerator(), 0, 1, run_event)
        assert closed_count[0] == 1


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


class GeneratorWithSmallImages(Subscriptable):
    def __init__(self, elements=10, blob_size=10) -> None:
        super().__init__()
        self.elements = elements
        self.blob_size = blob_size

    def __len__(self):
        return self.elements

    def getitem(self, subscript):
        query = [{"AddImage": {}}]
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


def test_dynamic_batching_add_image():
    db = MockClient()
    db.queries = []

    # 10 elements, 10 bytes each
    generator = GeneratorWithSmallImages(10, 10)
    querier = ParallelQuery(db)
    db.queries = []

    # Expected item size: len(str([{"AddImage": {}}])) -> 18 + 10 bytes blob = 28 bytes.
    # With a limit of 100 bytes, we should be able to fit 3 items per batch (3 * 28 = 84 bytes).
    # Since batchsize=5 is larger than 3, the max_bytes_per_batch limit will be the bottleneck,
    # producing 3, 3, 3, 1 item batches.
    querier.query(generator, batchsize=5, numthreads=1,
                  max_bytes_per_batch=100)

    # It should have succeeded in processing all queries
    assert querier.get_succeeded_queries() == 10
    assert len(db.queries) == 4
    for q in db.queries[:-1]:
        assert len(q) == 3
    assert len(db.queries[-1]) == 1


def test_dynamic_batching_add_image_variable_sizes():
    db = MockClient()
    db.queries = []

    # 1. Smaller images (2 bytes each) -> larger batches
    generator_small = GeneratorWithSmallImages(10, 2)
    querier_small = ParallelQuery(db)
    db.queries = []

    # Expected item size: 18 + 2 = 20 bytes.
    # Max bytes = 100 -> 100 // 20 = 5 items per batch.
    querier_small.query(generator_small, batchsize=10,
                        numthreads=1, max_bytes_per_batch=100)

    assert querier_small.get_succeeded_queries() == 10
    assert len(db.queries) == 2
    for q in db.queries:
        assert len(q) == 5

    # 2. Larger images (32 bytes each) -> smaller batches
    generator_large = GeneratorWithSmallImages(10, 32)
    db.queries = []
    querier_large = ParallelQuery(db)
    db.queries = []

    # Expected item size: 18 + 32 = 50 bytes.
    # Max bytes = 100 -> 100 // 50 = 2 items per batch.
    querier_large.query(generator_large, batchsize=10,
                        numthreads=1, max_bytes_per_batch=100)

    assert querier_large.get_succeeded_queries() == 10
    assert len(db.queries) == 5
    for q in db.queries:
        assert len(q) == 2


def test_dask_dry_run(db: Connector):
    from aperturedb.ParallelLoader import ParallelLoader
    from aperturedb.EntityDataCSV import EntityDataCSV
    from aperturedb.Utils import Utils
    utils = Utils(db)

    # Ensure starting clean
    utils.remove_entities(class_name="Person")

    # Create generator with Dask
    generator = EntityDataCSV(
        "./input/persons.adb.csv", use_dask=True, blobs_relative_to_csv=True)

    # Run ParallelLoader with dry_run=True
    loader = ParallelLoader(db, dry_run=True)
    loader.ingest(generator, batchsize=503, numthreads=4, stats=True)

    # Verify no objects were created
    res, _ = db.query(
        [{"FindEntity": {"with_class": "Person", "results": {"count": True}}}])
    assert db.last_query_ok(), res
    assert res[0]["FindEntity"]["count"] == 0
