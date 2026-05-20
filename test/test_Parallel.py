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

    def test_transformers(self, db: Connector):
        """
        Verifies that transformers are correctly applied.
        """
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)
        
        loader = ParallelLoader(db)
        loader.ingest(generator, batchsize=2, numthreads=2, stats=False, transformers=[DummyTransformer])
        
        assert loader.get_succeeded_queries() > 0

    def test_transformers_rejects_dask(self, db: Connector):
        elements = 10
        generator = GeneratorWithErrors(elements=elements, error_pct=0)
        generator.use_dask = True
        
        loader = ParallelLoader(db)
        try:
            loader.ingest(generator, batchsize=2, numthreads=2, stats=False, transformers=[DummyTransformer])
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Transformers cannot be used with Dask" in str(e)
