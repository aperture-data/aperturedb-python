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

    def test_parallel_query_worker_closes_connection(self, db, monkeypatch):
        from aperturedb.ParallelQuery import ParallelQuery
        from aperturedb.QueryGenerator import QueryGenerator
        import threading

        class MockQueryGenerator(QueryGenerator):
            def __len__(self):
                return 2

            def getitem(self, idx):
                if idx == 1:
                    raise Exception("Simulated do_batch exception")
                return [{"FindImage": {}}], []

        closed_count = [0]
        original_clone = db.clone

        def mock_clone():
            cloned = original_clone()
            original_close = cloned.close

            def mock_close():
                closed_count[0] += 1
                original_close()
            cloned.close = mock_close
            return cloned
        monkeypatch.setattr(db, "clone", mock_clone)

        # Test exception in do_batch
        pq = ParallelQuery(db)
        pq.query(MockQueryGenerator(), batchsize=1, numthreads=1)
        # Should close even if exception occurred in do_batch
        assert closed_count[0] == 1

        # Test early exit when run_event is cleared
        closed_count[0] = 0
        run_event = threading.Event()
        run_event.clear() # Not set, so worker breaks immediately
        
        # worker signature: worker(self, thid: int, generator, start: int, end: int, run_event)
        pq.worker(0, MockQueryGenerator(), 0, 1, run_event)
        assert closed_count[0] == 1
