import logging
import random
import pytest

from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Subscriptable import Subscriptable

logger = logging.getLogger(__name__)

# Tests for parallel which don't involve data.


class DummyGeneratorDaskBacked(Subscriptable):
    def __init__(self):
        super().__init__()
        self.use_dask = True
        class DummyDF:
            def map_partitions(self):
                pass
        self.df = DummyDF()

    def __len__(self):
        return 1

    def getitem(self, subscript):
        return [], []


class DummyGeneratorPandasBacked(Subscriptable):
    def __init__(self, with_use_dask_attr=False):
        super().__init__()
        if with_use_dask_attr:
            self.use_dask = False
        class DummyDF:
            pass
        self.df = DummyDF()

    def __len__(self):
        return 1

    def getitem(self, subscript):
        return [], []


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

    def test_use_dask_override_pandas_generator_raises(self, db: Connector):
        # loader-level use_dask=True + pandas generator raises ValueError
        querier = ParallelQuery(db, dry_run=True, use_dask=True)
        generator = DummyGeneratorPandasBacked()
        with pytest.raises(ValueError, match="generator must have a Dask DataFrame"):
            querier.query(generator)

    def test_use_dask_override_dask_generator_fails_fast(self, db: Connector):
        # loader-level use_dask=False + dask generator fails fast
        querier = ParallelQuery(db, dry_run=True, use_dask=False)
        generator = DummyGeneratorDaskBacked()
        with pytest.raises(ValueError, match="Cannot run with use_dask=False when the generator is dask-backed"):
            querier.query(generator)

    def test_use_dask_override_fallback_dask_generator(self, db: Connector):
        from unittest.mock import patch
        # use_dask=None still falls back to generator.use_dask
        querier = ParallelQuery(db, dry_run=True, use_dask=None)
        generator = DummyGeneratorDaskBacked()
        
        # We mock daskManager.run so it doesn't actually try to run map_partitions
        # But we want to ensure it passes the ValueError validations
        with patch("aperturedb.DaskManager.DaskManager.run") as mock_dask_run:
            mock_dask_run.return_value = ([], 0)
            
            querier.query(generator)
            assert querier.use_dask is None
            mock_dask_run.assert_called_once()

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
