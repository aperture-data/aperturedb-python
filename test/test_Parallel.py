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

    def test_progress_callback_and_log(self, caplog):
        """
        Verifies that progress_callback is called and log_progress logs appropriately.
        """
        from unittest.mock import MagicMock
        db = MagicMock()
        db.query.return_value = ([{"GetSchema": {}}], [])
        db.clone.return_value = db
        try:
            elements = 10
            batchsize = 2
            numthreads = 2

            generator = GeneratorWithErrors(elements=elements, error_pct=0)
            querier = ParallelQuery(db, dry_run=True)

            callback_calls = []

            def my_callback(worker_id, batch_index, total_batches, batch_start, batch_end, worker_stats, errors):
                callback_calls.append({
                    "worker_id": worker_id,
                    "batch_index": batch_index,
                    "total_batches": total_batches,
                    "batch_start": batch_start,
                    "batch_end": batch_end,
                    "worker_stats": worker_stats,
                    "errors": errors
                })

            import logging
            with caplog.at_level(logging.INFO):
                querier.query(generator, batchsize=batchsize,
                              numthreads=numthreads,
                              stats=True,
                              progress_callback=my_callback,
                              log_progress=True)

            import math
            elements_per_thread = math.ceil(elements / numthreads)
            expected_batches_per_thread = elements_per_thread // batchsize + (1 if elements_per_thread % batchsize > 0 else 0)
            expected_total_calls = expected_batches_per_thread * numthreads

            assert len(callback_calls) == expected_total_calls

            for call in callback_calls:
                assert "worker_id" in call
                assert "total_batches" in call
                assert call["total_batches"] == expected_batches_per_thread
                assert "batch_start" in call
                assert "batch_end" in call
                # Check that batch limits align with batch_index
                expected_start = (call["worker_id"] * elements_per_thread) + call["batch_index"] * batchsize
                assert call["batch_start"] == expected_start
                expected_end = min(expected_start + batchsize, (call["worker_id"] + 1) * elements_per_thread)
                assert call["batch_end"] == expected_end
                assert "worker_stats" in call
                assert "errors" in call

            log_messages = [record.message for record in caplog.records]
            completed_logs = [
                msg for msg in log_messages if "completed batch" in msg]
            assert len(completed_logs) == expected_total_calls

        except Exception as e:
            print(e)
            assert False
