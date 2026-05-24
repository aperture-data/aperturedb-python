import logging
import random

from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.Subscriptable import Subscriptable
from aperturedb.transformers.transformer import Transformer

logger = logging.getLogger(__name__)


class DummyTransformer(Transformer):
    def __init__(self, generator, client=None):
        super().__init__(generator, client=client)
        assert client is not None, "Client was not passed to transformer!"

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
        try:
            loader.ingest(generator, batchsize=2, numthreads=2,
                          stats=False, transformers=[DummyTransformer])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Transformers cannot be used with Dask" in str(e)

    def test_transformers_equivalence(self, db: Connector):
        '''
        Verifies that using transformers parameter is equivalent to manual wrapping.
        '''
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
